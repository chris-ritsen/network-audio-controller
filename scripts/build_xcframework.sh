#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
core="$root/packages/netaudio-core"
engine="$root/packages/netaudio-engine"
target_directory="$root/target"
framework_path="$root/dist/netaudio_engine.xcframework"
headers="$root/dist/netaudio_engine.headers"
source_paths=(
    LICENSE
    Cargo.toml
    Cargo.lock
    packages/netaudio-core
    packages/netaudio-daemon
    packages/netaudio-engine
    packages/netaudio/src/netaudio/daemon/http/webapp
    scripts/build_xcframework.sh
)

if ! source_commit="$(git -C "$root" rev-parse --verify HEAD)"; then
    echo "refusing to build XCFramework without a committed source revision" >&2
    exit 1
fi

repository_changes="$(git -C "$root" status --porcelain=v1 --untracked-files=all -- "${source_paths[@]}")"
if [[ -n "$repository_changes" ]]; then
    echo "refusing to build XCFramework from a dirty source tree" >&2
    exit 1
fi

source_tree="$(git -C "$root" ls-tree HEAD -- "${source_paths[@]}" | shasum -a 256 | awk '{print $1}')"

export IPHONEOS_DEPLOYMENT_TARGET="${IPHONEOS_DEPLOYMENT_TARGET:-15.0}"

cbindgen --config "$core/cbindgen.toml" --crate netaudio-core --output "$core/include/netaudio_core.h" "$core"
cbindgen --config "$engine/cbindgen.toml" --crate netaudio-engine --output "$engine/include/netaudio_engine.h" "$engine"

rm -rf "$headers" "$framework_path" "$root/dist/netaudio_engine.xcframework.zip"
mkdir -p "$headers"
cp "$core/include/netaudio_core.h" "$engine/include/netaudio_engine.h" "$headers/"
cat > "$headers/module.modulemap" <<'MODULEMAP'
module netaudio_core {
    header "netaudio_core.h"
    export *
}

module netaudio_engine {
    header "netaudio_engine.h"
    export *
}
MODULEMAP

libraries=()
for target in aarch64-apple-ios aarch64-apple-ios-macabi aarch64-apple-ios-sim; do
    cargo build --profile framework --locked --package netaudio-engine --target-dir "$target_directory" --target "$target"
    libraries+=(-library "$target_directory/$target/framework/libnetaudio_engine.a" -headers "$headers")
done

xcodebuild -create-xcframework "${libraries[@]}" -output "$framework_path"
rm -rf "$headers"

application_binary_interface_version="$(sed -nE 's/^pub const NETAUDIO_ABI_VERSION: u32 = ([0-9]+);$/\1/p' "$core/src/ffi/mod.rs")"
core_version="$(sed -nE 's/^version = "([^"]+)"$/\1/p' "$core/Cargo.toml" | head -n 1)"
engine_version="$(sed -nE 's/^version = "([^"]+)"$/\1/p' "$root/packages/netaudio-daemon/Cargo.toml" | head -n 1)"
if [[ -z "$application_binary_interface_version" || -z "$core_version" || -z "$engine_version" ]]; then
    echo "could not determine the versions and ABI for XCFramework provenance" >&2
    exit 1
fi

cp "$root/LICENSE" "$framework_path/LICENSE"

checksum() {
    shasum -a 256 "$framework_path/$1" | awk '{print $1}'
}

cat > "$framework_path/PROVENANCE.md" <<EOF
# netaudio-engine artifact provenance

- Source commit: \`$source_commit\`
- Source tree: \`$source_tree\`
- Core version: \`$core_version\`
- Engine version: \`$engine_version\`
- ABI version: \`$application_binary_interface_version\`
- Build script: \`scripts/build_xcframework.sh\`
- Cargo.lock SHA-256: \`$(shasum -a 256 "$root/Cargo.lock" | awk '{print $1}')\`

## Artifact checksums

| Artifact | SHA-256 |
| --- | --- |
| \`ios-arm64/libnetaudio_engine.a\` | \`$(checksum ios-arm64/libnetaudio_engine.a)\` |
| \`ios-arm64-maccatalyst/libnetaudio_engine.a\` | \`$(checksum ios-arm64-maccatalyst/libnetaudio_engine.a)\` |
| \`ios-arm64-simulator/libnetaudio_engine.a\` | \`$(checksum ios-arm64-simulator/libnetaudio_engine.a)\` |
| \`Info.plist\` | \`$(checksum Info.plist)\` |
| \`netaudio_core.h\` | \`$(checksum ios-arm64/Headers/netaudio_core.h)\` |
| \`netaudio_engine.h\` | \`$(checksum ios-arm64/Headers/netaudio_engine.h)\` |
| \`module.modulemap\` | \`$(checksum ios-arm64/Headers/module.modulemap)\` |
EOF

cd "$root/dist"
zip -qry netaudio_engine.xcframework.zip netaudio_engine.xcframework
shasum -a 256 netaudio_engine.xcframework.zip
