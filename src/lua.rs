//! A Lua script that redissun sends with `EVALSHA`, loading it when Redis does not know it.

/// A script and its SHA-1, which `EVALSHA` names it by.
pub struct Lua {
    source: String,
    sha1: String,
}

impl Lua {
    pub(crate) fn from_lua(source: impl Into<String>) -> Self {
        let source = source.into();
        let sha1 = sha1_smol::Sha1::from(source.as_bytes())
            .digest()
            .to_string();
        Self { source, sha1 }
    }

    /// The lowercase hex SHA-1 of the source.
    pub(crate) fn sha1(&self) -> &str {
        &self.sha1
    }

    pub(crate) fn source(&self) -> &str {
        &self.source
    }
}

#[cfg(test)]
mod tests {
    use super::Lua;

    #[test]
    fn the_sha_is_the_hex_sha1_of_the_source() {
        let lua = Lua::from_lua("return 1");
        assert_eq!(lua.sha1(), "e0e1f9fabfc9d4800c877a703b823ac0578ff8db");
        assert_eq!(lua.source(), "return 1");
    }
}
