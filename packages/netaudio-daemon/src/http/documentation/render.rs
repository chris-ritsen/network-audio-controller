use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

const STYLE: &str = include_str!("../../../assets/api-documentation.css");

pub(super) fn pages(document: &Value) -> Result<BTreeMap<String, String>> {
    let mut pages = BTreeMap::new();
    let mut index = String::new();
    paragraph(&mut index, text(&document["info"], "description"));
    index.push_str("<p><a href=\"/openapi.json\" download=\"netaudio-openapi.json\">Download OpenAPI JSON</a></p><h2>Guides</h2><ul class=\"guides\">");

    for (identifier, guide) in document["x-guides"]
        .as_object()
        .context("API guides missing")?
    {
        let path = format!("/api/docs/guides/{}", segment(identifier));
        let title = text(guide, "title");
        write!(
            index,
            "<li><a href=\"{}\">{}</a></li>",
            escape(&path),
            escape(title)
        )?;
        let mut content = String::new();

        for paragraph_value in guide["paragraphs"]
            .as_array()
            .context("guide paragraphs missing")?
        {
            paragraph(
                &mut content,
                paragraph_value
                    .as_str()
                    .context("invalid guide paragraph")?,
            );
        }

        pages.insert(path, page(title, &content));
    }

    index.push_str("</ul><h2>Operations</h2>");
    let mut groups = BTreeMap::<String, Vec<String>>::new();

    for (path, methods) in document["paths"].as_object().context("API paths missing")? {
        for (method, operation) in methods.as_object().context("invalid API path")? {
            let identifier = text(operation, "operationId");
            let title = text(operation, "summary");
            let page_path = format!("/api/docs/operations/{}", segment(identifier));
            let mut content = format!(
                "<p class=\"endpoint\"><strong>{}</strong> <code>{}</code></p>",
                escape(&method.to_uppercase()),
                escape(path),
            );
            paragraph(&mut content, text(operation, "description"));
            let security = operation.get("security").unwrap_or(&document["security"]);

            if security
                .as_array()
                .is_some_and(|requirements| !requirements.is_empty())
            {
                content.push_str("<h2>Authentication</h2>");
                paragraph(
                    &mut content,
                    "Use one of these security requirements. See the authentication and trust guide for scope and transport details.",
                );
                json(&mut content, security)?;
            }

            if let Some(parameters) = operation["parameters"].as_array() {
                content.push_str("<h2>Parameters</h2><dl class=\"fields\">");

                for parameter in parameters {
                    write!(
                        content,
                        "<div><dt><code>{}</code> ({}, {})</dt><dd>",
                        escape(text(parameter, "name")),
                        escape(text(parameter, "in")),
                        if parameter["required"] == true {
                            "required"
                        } else {
                            "optional"
                        },
                    )?;
                    paragraph(&mut content, text(parameter, "description"));
                    schema(&mut content, &parameter["schema"], document)?;
                    content.push_str("</dd></div>");
                }

                content.push_str("</dl>");
            }

            if let Some(request) = operation.get("requestBody") {
                content.push_str("<h2>Request body</h2>");
                paragraph(&mut content, text(request, "description"));
                representations(&mut content, request, document)?;
            }

            content.push_str("<h2>Responses</h2>");

            for (status, response) in operation["responses"]
                .as_object()
                .context("responses missing")?
            {
                let response = resolve(response, document);
                write!(
                    content,
                    "<section class=\"response\"><h3>{}</h3>",
                    escape(status)
                )?;
                paragraph(&mut content, text(response, "description"));
                representations(&mut content, response, document)?;

                if let Some(headers) = response.get("headers") {
                    content.push_str("<h4>Response headers</h4>");
                    json(&mut content, headers)?;
                }

                content.push_str("</section>");
            }

            pages.insert(page_path.clone(), page(title, &content));
            let tag = operation["tags"][0].as_str().unwrap_or("Other");
            groups.entry(tag.to_owned()).or_default().push(format!(
                "<li><a href=\"{}\"><span class=\"method\">{}</span><code>{}</code><span>{}</span></a></li>",
                escape(&page_path), escape(&method.to_uppercase()), escape(path), escape(title),
            ));
        }
    }

    for (group, operations) in groups {
        write!(
            index,
            "<h3>{}</h3><ul class=\"operations\">{}</ul>",
            escape(&group),
            operations.concat()
        )?;
    }

    index.push_str("<h2>Data schemas</h2><ul class=\"schemas\">");

    for (name, definition) in document["components"]["schemas"]
        .as_object()
        .context("API schemas missing")?
    {
        let path = format!("/api/docs/schemas/{}", segment(name));
        write!(
            index,
            "<li><a href=\"{}\">{}</a></li>",
            escape(&path),
            escape(name)
        )?;
        let mut content = String::new();
        schema(&mut content, definition, document)?;
        pages.insert(path, page(name, &content));
    }

    index.push_str("</ul>");
    pages.insert("/api/docs".to_owned(), page("HTTP API reference", &index));
    Ok(pages)
}

fn representations(output: &mut String, value: &Value, document: &Value) -> Result<()> {
    if let Some(content) = value["content"].as_object() {
        for (media_type, representation) in content {
            write!(output, "<h4><code>{}</code></h4>", escape(media_type))?;
            schema(output, &representation["schema"], document)?;

            if let Some(example) = representation.get("example") {
                output.push_str("<h4>Example</h4>");
                json(output, example)?;
            }
        }
    }

    Ok(())
}

fn schema(output: &mut String, value: &Value, document: &Value) -> Result<()> {
    let resolved = resolve(value, document);
    paragraph(output, text(resolved, "description"));
    let required = resolved["required"].as_array();

    if let Some(properties) = resolved["properties"].as_object() {
        output.push_str("<dl class=\"fields\">");

        for (name, definition) in properties {
            let required = required.is_some_and(|values| values.iter().any(|value| value == name));
            write!(
                output,
                "<div><dt><code>{}</code> ({})</dt><dd>",
                escape(name),
                if required { "required" } else { "optional" }
            )?;
            paragraph(output, text(definition, "description"));
            json(output, definition)?;
            links(output, definition)?;
            output.push_str("</dd></div>");
        }

        output.push_str("</dl><details><summary>Complete JSON Schema</summary>");
        json(output, resolved)?;
        output.push_str("</details>");
    } else {
        json(output, resolved)?;
    }

    links(output, value)?;
    Ok(())
}

fn resolve<'a>(value: &'a Value, document: &'a Value) -> &'a Value {
    value["$ref"]
        .as_str()
        .and_then(|reference| reference.strip_prefix('#'))
        .and_then(|pointer| document.pointer(pointer))
        .unwrap_or(value)
}

fn links(output: &mut String, value: &Value) -> Result<()> {
    let mut references = BTreeSet::new();
    collect_references(value, &mut references);

    if !references.is_empty() {
        output.push_str("<p>Related schemas: ");

        for (index, name) in references.iter().enumerate() {
            if index != 0 {
                output.push_str(", ");
            }

            write!(
                output,
                "<a href=\"/api/docs/schemas/{}\">{}</a>",
                segment(name),
                escape(name)
            )?;
        }

        output.push_str("</p>");
    }

    Ok(())
}

fn collect_references<'a>(value: &'a Value, references: &mut BTreeSet<&'a str>) {
    match value {
        Value::Object(fields) => {
            if let Some(name) = fields
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix("#/components/schemas/"))
            {
                references.insert(name);
            }

            for value in fields.values() {
                collect_references(value, references);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_references(value, references);
            }
        }
        _ => {}
    }
}

fn page(title: &str, content: &str) -> String {
    let version = crate::http::versioning::CONTRACT_VERSION;
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{} · NetAudio API</title><style>{STYLE}</style></head><body><a class=\"skip\" href=\"#content\">Skip to content</a><header><nav aria-label=\"API documentation\"><a href=\"/api/docs\">NetAudio API</a><a href=\"/openapi.json\">OpenAPI JSON</a></nav></header><main id=\"content\"><p class=\"release\">Public API {version}</p><h1>{}</h1>{content}</main></body></html>",
        escape(title),
        escape(title),
    )
}

fn json(output: &mut String, value: &Value) -> Result<()> {
    write!(
        output,
        "<pre><code>{}</code></pre>",
        escape(&serde_json::to_string_pretty(value)?)
    )?;
    Ok(())
}

fn paragraph(output: &mut String, value: &str) {
    if !value.is_empty() {
        output.push_str("<p>");
        output.push_str(&escape(value));
        output.push_str("</p>");
    }
}

fn text<'a>(value: &'a Value, name: &str) -> &'a str {
    value[name].as_str().unwrap_or_default()
}

fn segment(value: &str) -> String {
    const ENCODED: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.');
    percent_encoding::utf8_percent_encode(value, ENCODED).to_string()
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
