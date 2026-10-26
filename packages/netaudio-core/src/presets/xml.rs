use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(super) struct Element {
    pub name: String,
    pub namespace: Option<String>,
    namespaces: BTreeMap<String, String>,
    pub attributes: BTreeMap<String, String>,
    pub contents: Vec<Content>,
}

#[derive(Clone, Debug)]
pub(super) enum Content {
    Element(Element),
    Text(String),
    Comment(String),
    ProcessingInstruction(String),
}

impl Element {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            namespaces: BTreeMap::new(),
            attributes: BTreeMap::new(),
            contents: Vec::new(),
        }
    }

    pub fn children<'a>(&'a self, name: &str) -> impl Iterator<Item = &'a Element> {
        let name = name.to_owned();
        self.contents
            .iter()
            .filter_map(move |content| match content {
                Content::Element(element)
                    if element.name == name && element.namespace.is_none() =>
                {
                    Some(element)
                }
                _ => None,
            })
    }

    pub fn one(&self, name: &str) -> Result<Option<&Element>, String> {
        let mut children = self.children(name);
        let first = children.next();

        if children.next().is_some() {
            return Err(format!("duplicate {name} element"));
        }

        Ok(first)
    }

    pub fn text(&self) -> Result<String, String> {
        let mut text = String::new();

        for content in &self.contents {
            match content {
                Content::Text(value) => text.push_str(value),
                Content::Element(_) => return Err(format!("{} must contain text only", self.name)),
                _ => {}
            }
        }

        Ok(text)
    }

    pub fn child_text(&self, name: &str) -> Result<Option<String>, String> {
        self.one(name)?.map(Self::text).transpose()
    }

    pub fn remove(&mut self, name: &str) {
        self.contents.retain(|content| !matches!(content, Content::Element(element) if element.name == name && element.namespace.is_none()));
    }

    pub fn push(&mut self, element: Element) {
        self.contents.push(Content::Element(element));
    }

    pub fn set_text(&mut self, name: &str, text: impl Into<String>) {
        let text = text.into();

        for content in &mut self.contents {
            if let Content::Element(element) = content {
                if element.name == name && element.namespace.is_none() {
                    element.contents = vec![Content::Text(text)];
                    return;
                }
            }
        }

        let mut element = Self::new(name);
        element.contents = vec![Content::Text(text)];
        self.push(element);
    }

    pub fn render(&self) -> String {
        let mut output = String::new();
        self.write(&mut output, &BTreeMap::new());
        output
    }

    fn write(&self, output: &mut String, inherited: &BTreeMap<String, String>) {
        let mut namespaces = inherited.clone();
        let mut attributes = self.attributes.clone();

        for (name, value) in &self.namespaces {
            if inherited.get(name) != Some(value) {
                attributes.insert(name.clone(), value.clone());
            }

            namespaces.insert(name.clone(), value.clone());
        }

        if !self.name.contains(':')
            && self.namespace.is_none()
            && namespaces
                .get("xmlns")
                .is_some_and(|value| !value.is_empty())
        {
            attributes.insert("xmlns".into(), String::new());
            namespaces.insert("xmlns".into(), String::new());
        }

        output.push('<');
        output.push_str(&self.name);

        for (name, value) in &attributes {
            output.push(' ');
            output.push_str(name);
            output.push_str("=\"");
            output.push_str(&escape(value));
            output.push('"');
        }

        if self.contents.is_empty() {
            output.push_str("/>");
            return;
        }

        output.push('>');

        for content in &self.contents {
            match content {
                Content::Element(element) => element.write(output, &namespaces),
                Content::Text(text) => output.push_str(&escape(text)),
                Content::Comment(text) => {
                    output.push_str("<!--");
                    output.push_str(text);
                    output.push_str("-->");
                }
                Content::ProcessingInstruction(text) => {
                    output.push_str("<?");
                    output.push_str(text);
                    output.push_str("?>");
                }
            }
        }

        output.push_str("</");
        output.push_str(&self.name);
        output.push('>');
    }
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\r', "&#13;")
        .replace('\n', "&#10;")
        .replace('\t', "&#9;")
}

pub(super) fn parse(content: &str) -> Result<Element, String> {
    if content.len() > super::MAXIMUM_BYTES {
        return Err("preset XML must be no larger than 4 MiB".into());
    }

    let uppercase = content.to_ascii_uppercase();

    if uppercase.contains("<!DOCTYPE") || uppercase.contains("<!ENTITY") {
        return Err("preset XML must not contain document type or entity declarations".into());
    }

    let document = roxmltree::Document::parse_with_options(
        content,
        roxmltree::ParsingOptions {
            allow_dtd: false,
            nodes_limit: 131_072,
        },
    )
    .map_err(|error| format!("invalid preset XML: {error}"))?;
    owned_element(document.root_element(), content, 0)
}

fn owned_element(
    node: roxmltree::Node<'_, '_>,
    source: &str,
    depth: usize,
) -> Result<Element, String> {
    if depth >= 64 {
        return Err("preset XML exceeds its nesting limit".into());
    }

    let opening = &source[node.range().start + 1..node.range().end];
    let name = opening
        .split(|character: char| character.is_whitespace() || character == '/' || character == '>')
        .next()
        .ok_or("missing XML element name")?;
    let mut element = Element::new(name);
    element.namespace = node.tag_name().namespace().map(str::to_owned);
    element.namespaces = node
        .namespaces()
        .map(|namespace| {
            let name = namespace
                .name()
                .map(|prefix| format!("xmlns:{prefix}"))
                .unwrap_or_else(|| "xmlns".into());
            (name, namespace.uri().to_owned())
        })
        .collect();

    for attribute in node.attributes() {
        element.attributes.insert(
            source[attribute.range_qname()].into(),
            attribute.value().into(),
        );
    }

    for child in node.children() {
        let content = match child.node_type() {
            roxmltree::NodeType::Element => {
                Content::Element(owned_element(child, source, depth + 1)?)
            }
            roxmltree::NodeType::Text => Content::Text(child.text().unwrap_or_default().into()),
            roxmltree::NodeType::Comment => {
                Content::Comment(child.text().unwrap_or_default().into())
            }
            roxmltree::NodeType::PI => {
                let instruction = child.pi().ok_or("invalid XML processing instruction")?;
                Content::ProcessingInstruction(match instruction.value {
                    Some(value) => format!("{} {value}", instruction.target),
                    None => instruction.target.into(),
                })
            }
            roxmltree::NodeType::Root => return Err("unexpected nested XML root".into()),
        };
        element.contents.push(content);
    }

    Ok(element)
}
