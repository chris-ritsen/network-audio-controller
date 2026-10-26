use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub path: String,
    pub description: String,
    pub input_schema: Value,
    pub destructive: bool,
    pub method: String,
    pub requires_confirmation: bool,
    pub read_only: bool,
    pub defaults: Map<String, Value>,
    pub meta: Map<String, Value>,
}

pub struct Tool {
    pub definition: ToolDefinition,
    pub document: Value,
    validator: jsonschema::Validator,
}

impl Tool {
    fn new(definition: ToolDefinition) -> Result<Self> {
        let validator = jsonschema::options()
            .with_pattern_options(jsonschema::PatternOptions::regex())
            .build(&definition.input_schema)
            .with_context(|| format!("compile schema for {}", definition.name))?;
        let mut document = json!({
            "name": definition.name,
            "description": definition.description,
            "inputSchema": definition.input_schema,
            "annotations": {
                "destructiveHint": definition.destructive,
                "readOnlyHint": definition.read_only,
            },
        });

        if !definition.meta.is_empty() {
            document["_meta"] = json!(definition.meta);
        }

        Ok(Self {
            definition,
            document,
            validator,
        })
    }

    pub fn argument_errors(&self, arguments: &Value) -> Vec<String> {
        self.validator
            .iter_errors(arguments)
            .map(|error| {
                let path = error.instance_path().to_string();
                let label = if path.is_empty() { "arguments" } else { &path };
                format!("{label}: {}", error.masked())
            })
            .collect()
    }
}

#[derive(Deserialize)]
pub struct PlannedOperation {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub features: Vec<String>,
    pub blocker: String,
    pub blocked_by: String,
    pub read_only: bool,
}

impl PlannedOperation {
    pub fn unavailable(&self) -> Value {
        json!({
            "error": "not_implemented",
            "operation": self.name,
            "blocker": self.blocker,
            "blocked_by": self.blocked_by,
            "features": self.features,
        })
    }
}

#[derive(Deserialize)]
pub struct Resource {
    pub uri: String,
    pub path: String,
    pub name: String,
    pub description: String,
    pub tool_name: String,
}

impl Resource {
    pub fn document(&self) -> Value {
        json!({
            "uri": self.uri,
            "name": self.name,
            "description": self.description,
            "mimeType": "application/json",
        })
    }
}

#[derive(Deserialize)]
struct DiscoveryRules {
    terms: BTreeMap<String, String>,
    stop_words: BTreeSet<String>,
    write_words: BTreeSet<String>,
    hints: BTreeMap<String, BTreeSet<String>>,
    aliases: BTreeMap<String, BTreeSet<String>>,
    categories: Vec<(String, Vec<String>)>,
    category_overrides: BTreeMap<String, String>,
    default_category: String,
    schema_limit: usize,
}

impl DiscoveryRules {
    fn words(&self, text: &str) -> BTreeSet<String> {
        raw_words(text)
            .filter(|word| !self.stop_words.contains(word))
            .map(|word| self.terms.get(&word).cloned().unwrap_or(word))
            .collect()
    }

    fn category(&self, name: &str) -> String {
        if let Some(category) = self.category_overrides.get(name) {
            return category.clone();
        }

        self.categories
            .iter()
            .find(|(_, fragments)| fragments.iter().any(|fragment| name.contains(fragment)))
            .map(|(category, _)| category.clone())
            .unwrap_or_else(|| self.default_category.clone())
    }
}

struct SearchEntry {
    name: String,
    category: String,
    planned: bool,
    read_only: bool,
    name_words: BTreeSet<String>,
    description_words: BTreeSet<String>,
    hint_words: BTreeSet<String>,
    alias_words: BTreeSet<String>,
    document: Value,
    schema: Value,
}

impl SearchEntry {
    fn score(&self, words: &BTreeSet<String>, wants_write: bool) -> usize {
        if words.iter().all(|word| {
            !self.name_words.contains(word)
                && !self.description_words.contains(word)
                && !self.hint_words.contains(word)
                && !self.alias_words.contains(word)
        }) {
            return 0;
        }

        let name_matches = words
            .iter()
            .filter(|word| self.name_words.contains(*word) || self.alias_words.contains(*word))
            .count();
        let mut score = 8 * name_matches
            + 2 * words.intersection(&self.description_words).count()
            + 4 * words.intersection(&self.hint_words).count();

        if words.iter().all(|word| {
            self.name_words.contains(word)
                || self.description_words.contains(word)
                || self.hint_words.contains(word)
                || self.alias_words.contains(word)
        }) {
            score += 5;
        }

        if self.read_only != wants_write {
            score += 3;
        }

        if self.planned {
            score = score.saturating_sub(8).max(1);
        }

        score
    }
}

#[derive(Deserialize)]
struct CatalogDocument {
    tools: Vec<ToolDefinition>,
    app_tools: Vec<ToolDefinition>,
    public_names: BTreeSet<String>,
    resources: Vec<Resource>,
    device_resource_template: Value,
    planned: Vec<PlannedOperation>,
    discovery: DiscoveryRules,
    device_record_fields: BTreeSet<String>,
}

pub struct Catalog {
    pub tools: BTreeMap<String, Tool>,
    pub app_tools: BTreeMap<String, Tool>,
    pub public_tools: Value,
    pub resources: Vec<Resource>,
    pub device_resource_template: Value,
    pub planned: BTreeMap<String, PlannedOperation>,
    pub device_record_fields: BTreeSet<String>,
    rules: DiscoveryRules,
    search: Vec<SearchEntry>,
    index: Value,
}

impl Catalog {
    pub fn load() -> Result<Self> {
        let document: CatalogDocument =
            serde_json::from_str(include_str!("../../assets/mcp/catalog.json"))?;
        let mut tools = BTreeMap::new();

        for definition in document.tools {
            let name = definition.name.clone();
            ensure!(!tools.contains_key(&name), "duplicate MCP operation {name}");
            tools.insert(name, Tool::new(definition)?);
        }

        ensure!(
            document
                .public_names
                .iter()
                .all(|name| tools.contains_key(name)),
            "public MCP catalog contains an unknown operation"
        );
        let mut public_tools: Vec<_> = tools
            .iter()
            .filter(|(name, _)| document.public_names.contains(*name))
            .map(|(_, tool)| tool.document.clone())
            .collect();
        let mut app_tools = BTreeMap::new();

        for definition in document.app_tools {
            let name = definition.name.clone();
            ensure!(
                !tools.contains_key(&name) && !app_tools.contains_key(&name),
                "duplicate MCP app operation {name}"
            );
            let tool = Tool::new(definition)?;
            public_tools.push(tool.document.clone());
            app_tools.insert(name, tool);
        }

        let public_tools = json!({"tools": public_tools});
        let mut planned = BTreeMap::new();

        for operation in document.planned {
            ensure!(
                !planned.contains_key(&operation.name) && !tools.contains_key(&operation.name),
                "duplicate planned MCP operation {}",
                operation.name
            );
            planned.insert(operation.name.clone(), operation);
        }

        let mut search = Vec::new();

        for tool in tools.values().filter(|tool| {
            !["discover_tools", "invoke_tool"].contains(&tool.definition.name.as_str())
        }) {
            let definition = &tool.definition;
            search.push(search_entry(
                &document.discovery,
                &definition.name,
                &definition.description,
                &definition.input_schema,
                definition.read_only,
                None,
            ));
        }

        for operation in planned.values() {
            search.push(search_entry(
                &document.discovery,
                &operation.name,
                &operation.description,
                &operation.input_schema,
                operation.read_only,
                Some(operation),
            ));
        }

        let mut categories = BTreeMap::<String, BTreeMap<&str, Vec<&str>>>::new();
        let mut ordered: Vec<_> = search.iter().collect();
        ordered.sort_by_key(|entry| &entry.name);

        for entry in ordered {
            categories
                .entry(entry.category.clone())
                .or_default()
                .entry(if entry.planned {
                    "planned"
                } else {
                    "available"
                })
                .or_default()
                .push(&entry.name);
        }

        let index = json!({
            "categories": categories,
            "next": "Query a category name for descriptions, or an exact operation name with include_schema=true for its arguments. Planned operations are designed but not built.",
        });
        Ok(Self {
            tools,
            app_tools,
            public_tools,
            resources: document.resources,
            device_resource_template: document.device_resource_template,
            planned,
            device_record_fields: document.device_record_fields,
            rules: document.discovery,
            search,
            index,
        })
    }

    pub fn discover(
        &self,
        query: &str,
        limit: usize,
        offset: usize,
        include_schema: bool,
    ) -> Value {
        if query.trim().is_empty() {
            return self.index.clone();
        }

        let exact = query.trim().to_lowercase().replace(' ', "_");
        let category = query.trim().to_lowercase();
        let words = self.rules.words(query);
        let wants_write = raw_words(query).any(|word| self.rules.write_words.contains(&word));
        let mut matches: Vec<_> = self
            .search
            .iter()
            .filter(|entry| entry.name == exact)
            .collect();

        if matches.is_empty() {
            matches = self
                .search
                .iter()
                .filter(|entry| entry.category.to_lowercase() == category)
                .collect();
            matches.sort_by_key(|entry| (entry.planned, &entry.name));

            if matches.is_empty() {
                if words.is_empty() {
                    matches = self.search.iter().collect();
                } else {
                    let mut ranked = self
                        .search
                        .iter()
                        .map(|entry| (entry.score(&words, wants_write), entry))
                        .filter(|(score, _)| *score > 0)
                        .collect::<Vec<_>>();
                    ranked.sort_by_key(|(score, entry)| {
                        (std::cmp::Reverse(*score), entry.planned, &entry.name)
                    });
                    matches = ranked.into_iter().map(|(_, entry)| entry).collect();
                }
            }
        }

        let selected: Vec<_> = matches
            .iter()
            .skip(offset)
            .take(limit)
            .enumerate()
            .map(|(position, entry)| {
                let mut record = entry.document.clone();

                if include_schema && position < self.rules.schema_limit {
                    record["input_schema"] = entry.schema.clone();
                }

                record
            })
            .collect();
        let next = offset.saturating_add(selected.len());
        let mut result = json!({
            "operations": selected,
            "matched": matches.len(),
            "next_offset": (next < matches.len()).then_some(next),
        });

        if include_schema && selected.len() > self.rules.schema_limit {
            result["schema_note"] = json!(format!(
                "schemas are included for the first {} matches; search for another by its name to get its schema",
                self.rules.schema_limit,
            ));
        }

        if matches.is_empty() {
            result["hint"] = json!(
                "Nothing matched; call discover_tools without a query to see every operation by category."
            );
        }

        result
    }
}

fn search_entry(
    rules: &DiscoveryRules,
    name: &str,
    description: &str,
    schema: &Value,
    read_only: bool,
    planned: Option<&PlannedOperation>,
) -> SearchEntry {
    let mut document = json!({
        "name": name, "description": description, "read_only": read_only,
        "required": schema.get("required"),
        "status": if planned.is_some() { "planned" } else { "available" },
    });

    if let Some(planned) = planned {
        document["blocker"] = json!(planned.blocker);
        document["blocked_by"] = json!(planned.blocked_by);
    }

    SearchEntry {
        name: name.to_owned(),
        category: rules.category(name),
        planned: planned.is_some(),
        read_only,
        name_words: rules.words(name),
        description_words: rules.words(description),
        hint_words: rules.hints.get(name).cloned().unwrap_or_default(),
        alias_words: rules.aliases.get(name).cloned().unwrap_or_default(),
        document,
        schema: schema.clone(),
    }
}

fn raw_words(text: &str) -> impl Iterator<Item = String> {
    text.to_ascii_lowercase()
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>()
        .into_iter()
}
