use std::io::{self, Read};

use serde::Serialize;
use sha2::{Digest, Sha256};

const CHUNK_LENGTH: usize = 1 << 20;
const MACOS_APPLE_SILICON: &str = "macOS (Apple silicon)";
const MACOS_INTEL: &str = "macOS (Intel)";
const WINDOWS: &str = "Windows";

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Release {
    pub platform: &'static str,
    pub version: &'static str,
}

const RELEASES: [(&str, Release); 18] = [
    (
        "0caa97f11f13e6af67da28c5aece8ec27023ed9a32e9b7591870bcb82c9b374c",
        Release {
            platform: WINDOWS,
            version: "4.10.0.3",
        },
    ),
    (
        "41045291e18255c1eb8e8cbc01bde93cbbb1ba8a7d5f312fd998d7244b79a161",
        Release {
            platform: WINDOWS,
            version: "4.4.2.2",
        },
    ),
    (
        "4515cd12b4b185e4b6317db6de7ea5306ba33381baa2fce4ee08f5fc1b6538ff",
        Release {
            platform: MACOS_APPLE_SILICON,
            version: "4.18.1.1",
        },
    ),
    (
        "49e7520c4f4031cfe1dfd7c8a9ff435754101dbbae13dfe48c4c1c74728b4446",
        Release {
            platform: WINDOWS,
            version: "4.1.0.5",
        },
    ),
    (
        "4ef04c583942c3962b8ed61af6765f06f4519eac5be9f8db546fed2bfd9273d8",
        Release {
            platform: WINDOWS,
            version: "4.10.0.3",
        },
    ),
    (
        "57aca23174d25be6d1f3e8ad242bacacc899a510402ded80768fbc3e492648e0",
        Release {
            platform: MACOS_APPLE_SILICON,
            version: "4.16.1.5",
        },
    ),
    (
        "7902bf615848e7c886675ee950ef92e38b6e919a370d580155530455a55362a1",
        Release {
            platform: MACOS_INTEL,
            version: "4.18.1.1",
        },
    ),
    (
        "9716f265abf0daddca8832d2fa5e9a63812f86b980c5b16553cf9ea70432ac99",
        Release {
            platform: WINDOWS,
            version: "4.2.3.1",
        },
    ),
    (
        "ad8cf496231e4dfa3e26ebaf60b0121d9ae51ed2ed0db0ea57f7d76c8bf407d3",
        Release {
            platform: WINDOWS,
            version: "4.18.1.2",
        },
    ),
    (
        "c00a9cf254f8cb3f2927015ba6bdabef468bce413a845624e076f9ef2db43881",
        Release {
            platform: MACOS_INTEL,
            version: "4.18.1.1",
        },
    ),
    (
        "c7715be1d14b423987d638f6a6abfc373835ddb09039ee65acbf773e23620340",
        Release {
            platform: MACOS_INTEL,
            version: "4.14.2.1",
        },
    ),
    (
        "d19828de52fb7656cc72e34c29c505948a191d10bc600ecda9e08620b79765b9",
        Release {
            platform: MACOS_APPLE_SILICON,
            version: "4.18.1.1",
        },
    ),
    (
        "d3e6ab7445fde907b9b6b58efabd1e2d19e37b6f4e1d4ed784ab85d85a64d7c5",
        Release {
            platform: WINDOWS,
            version: "4.18.1.2",
        },
    ),
    (
        "d5655cddbfbaaa9b34e3de6a7c226b1ef40eae0ee2d574705791417139e96cf9",
        Release {
            platform: WINDOWS,
            version: "4.4.2.2",
        },
    ),
    (
        "db9c60bcbb9b9715be17f3f7a1d2bd4e46d3bc1a9a60db758cb63eab81819b92",
        Release {
            platform: WINDOWS,
            version: "4.18.1.2",
        },
    ),
    (
        "f109cdb867ee747616ce381bd2f22ccf269ddc87a41a554d821574c522d3ef8b",
        Release {
            platform: WINDOWS,
            version: "4.1.0.5",
        },
    ),
    (
        "f76d442f1960df584ea4fdd047e9d25012ae2a1fd0b6741bc343233cc7ea2d35",
        Release {
            platform: MACOS_INTEL,
            version: "4.14.2.1",
        },
    ),
    (
        "fcbb29c4392b0486bddc76f0b5bc5f2441e4d336ca5e3b63759ce47616f748e0",
        Release {
            platform: WINDOWS,
            version: "4.2.3.1",
        },
    ),
];

pub fn identify(input: &mut dyn Read) -> io::Result<Option<Release>> {
    let mut chunk = vec![0_u8; CHUNK_LENGTH];
    let mut hasher = Sha256::new();

    loop {
        match input.read(&mut chunk)? {
            0 => break,
            length => hasher.update(&chunk[..length]),
        }
    }

    let digest: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(RELEASES
        .binary_search_by(|(known, _)| (*known).cmp(digest.as_str()))
        .ok()
        .map(|index| RELEASES[index].1))
}
