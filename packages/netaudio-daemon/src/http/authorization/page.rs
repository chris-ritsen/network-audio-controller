use super::{AUTHORIZE_PATH, ConsentRequest};
use crate::authorization::{READ_SCOPE, WRITE_SCOPE};

pub fn render(request: &ConsentRequest, host: &str, error: Option<&str>) -> String {
    let mut hidden = String::new();

    for (name, value) in [
        ("client_id", request.client_id.as_str()),
        ("code_challenge", request.challenge.as_str()),
        ("redirect_uri", request.redirect.as_str()),
        ("state", request.state.as_str()),
        ("response_type", "code"),
        ("code_challenge_method", "S256"),
    ] {
        hidden.push_str(&format!(
            "<input type=\"hidden\" name=\"{name}\" value=\"{}\">",
            escape(value)
        ));
    }

    let error = error
        .map(|message| format!("<p class=\"error\" role=\"alert\">{}</p>", escape(message)))
        .unwrap_or_default();
    let read_checked = if request.scope == READ_SCOPE {
        "checked"
    } else {
        ""
    };
    let write_checked = if request.scope == WRITE_SCOPE {
        "checked"
    } else {
        ""
    };
    let client = escape(&request.client_name);
    let host = escape(host);

    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>netaudio access</title>
<style>
body {{ font: 16px/1.5 -apple-system, system-ui, sans-serif; background: #111; color: #fff; margin: 0; padding: 24px; }}
main {{ max-width: 28rem; margin: 8vh auto; }}
h1 {{ font-size: 1.4rem; margin: 0 0 0.5rem; }}
p {{ margin: 0.5rem 0; }}
label {{ display: block; margin-top: 1.2rem; }}
input[type=password] {{ width: 100%; box-sizing: border-box; font: inherit; padding: 0.6rem; margin-top: 0.3rem; border: 1px solid #777; background: #1c1c1e; color: inherit; }}
fieldset {{ border: 1px solid #777; margin-top: 1.2rem; padding: 0.6rem 1rem; }}
legend {{ padding: 0 0.3rem; }}
.actions {{ display: flex; flex-wrap: wrap; gap: 0.8rem; margin-top: 1.4rem; }}
button {{ font: inherit; padding: 0.6rem 1.2rem; border: 1px solid #aaa; background: #2c2c2e; color: inherit; cursor: pointer; }}
:focus-visible {{ outline: 2px solid #fff; outline-offset: 3px; }}
.error {{ color: #ff8c8c; }}
</style>
</head>
<body>
<main>
<h1>{client} wants to control your Dante network</h1>
<p>through netaudio on {host}.</p>
{error}
<form method="post" action="{AUTHORIZE_PATH}">
{hidden}
<fieldset>
<legend>Allow it to</legend>
<label><input type="radio" name="scope" value="{WRITE_SCOPE}" {write_checked}> read and change devices, routing and settings</label>
<label><input type="radio" name="scope" value="{READ_SCOPE}" {read_checked}> only read devices, routes, levels and issues</label>
</fieldset>
<label>Login secret<input type="password" name="secret" autocomplete="current-password" required></label>
<div class="actions">
<button name="decision" value="allow">Allow</button>
<button name="decision" value="deny" formnovalidate>Deny</button>
</div>
</form>
</main>
</body>
</html>
"#
    )
}

fn escape(value: &str) -> String {
    let mut output = String::with_capacity(value.len());

    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&#39;"),
            _ => output.push(character),
        }
    }

    output
}
