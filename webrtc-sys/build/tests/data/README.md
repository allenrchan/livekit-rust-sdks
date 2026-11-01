These small archives contain test text, not WebRTC binaries.

- `prebuilt-valid.zip` uses DEFLATE and the prebuilt directory/header/license layout.
- `prebuilt-mismatched-headers.zip` has a central name of `linux-arm64-release/approved.txt` and a different, same-length local-header name. Extraction must fail before writing either name.
- `prebuilt-duplicate-names.zip` has two central entries named `linux-arm64-release/LICENSE.md`. Extraction must fail before writing a shadowed entry.

The malformed cases passed through zip 0.6.6 extraction before the validation repair. The valid case must continue to extract successfully.
