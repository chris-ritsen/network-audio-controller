pub fn decode_hexadecimal(encoded: &str) -> Vec<u8> {
    assert_eq!(encoded.len() % 2, 0, "odd-length hexadecimal input");
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hexadecimal input must be ascii");
            u8::from_str_radix(text, 16).expect("hexadecimal input must be valid")
        })
        .collect()
}
