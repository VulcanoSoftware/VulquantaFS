// Strips the credentials from a generated config object, so bug reports can
// be shared without the secrets.
//
// Key names stay intact so the report still reads as a config; non-empty
// credentials are replaced with ``<redacted>``. What counts as a secret by
// key name: api_hash, access_token and the tokens (Telegram bot, Discord),
// the user passwords, the jwt secret and an inline encryption passphrase. A
// passphrase_env stays readable: it names where the secret lives.
const REDACTED = "<redacted>";

const SECRET_YAML_KEYS = [
  "api_hash",
  "access_token",
  "bot_tokens",
  "passphrase",
  "password",
  "secret",
  "tokens",
];

export const redactSecrets = (node: unknown): unknown => {
  if (Array.isArray(node)) return node.map(redactSecrets);
  if (node === null || typeof node !== "object") return node;
  return Object.fromEntries(
    Object.entries(node).map(([key, value]) => {
      if (!SECRET_YAML_KEYS.includes(key)) return [key, redactSecrets(value)];
      return [
        key,
        Array.isArray(value)
          ? value.map((v) =>
              typeof v === "string" && v.trim() !== "" ? REDACTED : v
            )
          : typeof value === "string" && value.trim() !== ""
          ? REDACTED
          : value,
      ];
    })
  );
};
