use std::path::Path;

// The install snippets name the version by hand, so a release that forgets them fails here.
#[test]
fn install_snippets_name_the_current_version() {
    let current = format!(
        "\"{}.{}\"",
        env!("CARGO_PKG_VERSION_MAJOR"),
        env!("CARGO_PKG_VERSION_MINOR")
    );
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for file in [
        "README.md",
        "website/content/docs/getting-started/installation.mdx",
        "website/content/docs/guides/configuration.mdx",
    ] {
        let text = std::fs::read_to_string(root.join(file)).unwrap();
        for line in text.lines().filter(|line| line.starts_with("redissun = ")) {
            assert!(
                line.contains(&current),
                "{file} has `{line}`, expected {current}"
            );
        }
    }
}
