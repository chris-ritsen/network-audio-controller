#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
crate="$root/packages/netaudio-core"

export IPHONEOS_DEPLOYMENT_TARGET="${IPHONEOS_DEPLOYMENT_TARGET:-15.0}"

cbindgen --config "$crate/cbindgen.toml" --crate netaudio-core --output "$crate/include/netaudio_core.h" "$crate"

cargo build --release --manifest-path "$crate/Cargo.toml" --target aarch64-apple-ios
cargo build --release --manifest-path "$crate/Cargo.toml" --target aarch64-apple-ios-sim

rm -rf "$root/dist/netaudio_core.xcframework" "$root/dist/netaudio_core.xcframework.zip"
mkdir -p "$root/dist"

xcodebuild -create-xcframework \
    -library "$crate/target/aarch64-apple-ios/release/libnetaudio_core.a" -headers "$crate/include" \
    -library "$crate/target/aarch64-apple-ios-sim/release/libnetaudio_core.a" -headers "$crate/include" \
    -output "$root/dist/netaudio_core.xcframework"

cd "$root/dist"
zip -qry netaudio_core.xcframework.zip netaudio_core.xcframework
shasum -a 256 netaudio_core.xcframework.zip
