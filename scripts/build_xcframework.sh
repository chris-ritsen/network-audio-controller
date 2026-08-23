#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
crate="$root/packages/netaudio-core"

if ! source_commit="$(git -C "$root" rev-parse --verify HEAD)"; then
    echo "refusing to build XCFramework without a committed source revision" >&2
    exit 1
fi

repository_changes="$(
    git -C "$root" status --porcelain=v1 --untracked-files=all -- \
        LICENSE \
        packages/netaudio-core \
        scripts/build_xcframework.sh
)"
if [[ -n "$repository_changes" ]]; then
    echo "refusing to build XCFramework from a dirty source tree" >&2
    exit 1
fi

export IPHONEOS_DEPLOYMENT_TARGET="${IPHONEOS_DEPLOYMENT_TARGET:-15.0}"

cbindgen --config "$crate/cbindgen.toml" --crate netaudio-core --output "$crate/include/netaudio_core.h" "$crate"

cat > "$crate/include/module.modulemap" <<'MODULEMAP'
module netaudio_core {
    header "netaudio_core.h"
    export *
}
MODULEMAP

cargo build --release --manifest-path "$crate/Cargo.toml" --target aarch64-apple-ios
cargo build --release --manifest-path "$crate/Cargo.toml" --target aarch64-apple-ios-sim

rm -rf "$root/dist/netaudio_core.xcframework" "$root/dist/netaudio_core.xcframework.zip"
mkdir -p "$root/dist"

xcodebuild -create-xcframework \
    -library "$crate/target/aarch64-apple-ios/release/libnetaudio_core.a" -headers "$crate/include" \
    -library "$crate/target/aarch64-apple-ios-sim/release/libnetaudio_core.a" -headers "$crate/include" \
    -output "$root/dist/netaudio_core.xcframework"

framework_path="$root/dist/netaudio_core.xcframework"
application_binary_interface_version="$(sed -nE 's/^pub const NETAUDIO_ABI_VERSION: u32 = ([0-9]+);$/\1/p' "$crate/src/ffi/mod.rs")"
crate_version="$(sed -nE 's/^version = "([^"]+)"$/\1/p' "$crate/Cargo.toml" | head -n 1)"
if [[ -z "$application_binary_interface_version" || -z "$crate_version" ]]; then
    echo "could not determine the core version and ABI for XCFramework provenance" >&2
    exit 1
fi

cp "$root/LICENSE" "$framework_path/LICENSE"

device_library_checksum="$(shasum -a 256 "$framework_path/ios-arm64/libnetaudio_core.a" | awk '{print $1}')"
simulator_library_checksum="$(shasum -a 256 "$framework_path/ios-arm64-simulator/libnetaudio_core.a" | awk '{print $1}')"
framework_manifest_checksum="$(shasum -a 256 "$framework_path/Info.plist" | awk '{print $1}')"
header_checksum="$(shasum -a 256 "$framework_path/ios-arm64/Headers/netaudio_core.h" | awk '{print $1}')"
module_map_checksum="$(shasum -a 256 "$framework_path/ios-arm64/Headers/module.modulemap" | awk '{print $1}')"
cargo_lock_checksum="$(shasum -a 256 "$crate/Cargo.lock" | awk '{print $1}')"

cat > "$framework_path/PROVENANCE.md" <<EOF
# netaudio-core artifact provenance

- Source commit: \`$source_commit\`
- Crate version: \`$crate_version\`
- ABI version: \`$application_binary_interface_version\`
- Build script: \`scripts/build_xcframework.sh\`
- Cargo.lock SHA-256: \`$cargo_lock_checksum\`

## Artifact checksums

| Artifact | SHA-256 |
| --- | --- |
| \`ios-arm64/libnetaudio_core.a\` | \`$device_library_checksum\` |
| \`ios-arm64-simulator/libnetaudio_core.a\` | \`$simulator_library_checksum\` |
| \`Info.plist\` | \`$framework_manifest_checksum\` |
| \`netaudio_core.h\` | \`$header_checksum\` |
| \`module.modulemap\` | \`$module_map_checksum\` |
EOF

cd "$root/dist"
zip -qry netaudio_core.xcframework.zip netaudio_core.xcframework
shasum -a 256 netaudio_core.xcframework.zip
