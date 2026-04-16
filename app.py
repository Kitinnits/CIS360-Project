#!/usr/bin/env python3
"""Localhost web app for natural-language querying of data_fusion.db."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template_string, request


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data_fusion.db"

app = Flask(__name__)


PAGE_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>Natural Language Database Query</title>
	<style>
		:root {
			--bg: #f4f7fb;
			--card: #ffffff;
			--ink: #111827;
			--muted: #4b5563;
			--accent: #0d9488;
			--accent-dark: #0f766e;
			--border: #d1d5db;
			--error-bg: #fee2e2;
			--error-ink: #991b1b;
		}
		* { box-sizing: border-box; }
		body {
			margin: 0;
			font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
			color: var(--ink);
			background:
				radial-gradient(circle at 20% 10%, #d1fae5 0%, transparent 40%),
				radial-gradient(circle at 80% 90%, #bfdbfe 0%, transparent 35%),
				var(--bg);
			min-height: 100vh;
			padding: 24px;
		}
		.container {
			max-width: 980px;
			margin: 0 auto;
			background: var(--card);
			border: 1px solid var(--border);
			border-radius: 16px;
			box-shadow: 0 12px 30px rgba(17, 24, 39, 0.08);
			overflow: hidden;
		}
		.header {
			padding: 24px;
			background: linear-gradient(130deg, #0d9488, #0f766e);
			color: #ffffff;
		}
		.header h1 {
			margin: 0;
			font-size: 1.6rem;
		}
		.header p {
			margin: 8px 0 0;
			opacity: 0.95;
		}
		.content {
			padding: 24px;
		}
		form {
			display: grid;
			gap: 10px;
			margin-bottom: 20px;
		}
		input[type="text"],
		textarea {
			width: 100%;
			padding: 12px;
			border: 1px solid var(--border);
			border-radius: 10px;
			font-size: 1rem;
		}
		textarea {
			min-height: 110px;
			resize: vertical;
			font-family: inherit;
		}
		button {
			width: fit-content;
			background: var(--accent);
			color: #fff;
			border: none;
			border-radius: 10px;
			padding: 10px 16px;
			font-size: 0.95rem;
			cursor: pointer;
		}
		button:hover { background: var(--accent-dark); }
		.examples {
			margin: 0 0 20px;
			padding: 12px;
			background: #ecfeff;
			border: 1px solid #99f6e4;
			border-radius: 10px;
			color: #134e4a;
			line-height: 1.45;
		}
		.examples code {
			display: block;
			white-space: pre-wrap;
			margin: 6px 0;
			background: rgba(255, 255, 255, 0.85);
			border-radius: 8px;
			padding: 8px;
		}
		.examples button {
			display: block;
			width: 100%;
			text-align: left;
			margin: 8px 0 0;
			background: #ffffff;
			color: #134e4a;
			border: 1px solid #99f6e4;
			border-radius: 8px;
			padding: 10px 12px;
			cursor: pointer;
		}
		.examples button:hover {
			background: #f0fdfa;
		}
		.error {
			margin: 0 0 16px;
			padding: 10px;
			border-radius: 8px;
			background: var(--error-bg);
			color: var(--error-ink);
		}
		details.sql-block {
			margin: 0 0 12px;
			padding: 10px 12px;
			border: 1px solid var(--border);
			border-radius: 10px;
			background: #f9fafb;
		}
		details.sql-block summary {
			cursor: pointer;
			font-weight: 600;
			color: var(--muted);
		}
		.sql-code {
			margin: 10px 0 0;
			white-space: pre-wrap;
			word-break: break-word;
			font-family: Consolas, "Liberation Mono", Menlo, monospace;
			font-size: 0.85rem;
			color: #1f2937;
		}
		.meta {
			font-size: 0.9rem;
			color: var(--muted);
			margin-bottom: 12px;
		}
		.results-layout {
			display: grid;
			grid-template-columns: minmax(0, 1fr) 240px;
			gap: 16px;
			align-items: start;
		}
		.column-panel {
			position: sticky;
			top: 16px;
			border: 1px solid var(--border);
			border-radius: 12px;
			background: #f9fafb;
			padding: 12px;
		}
		.column-panel h2 {
			margin: 0 0 8px;
			font-size: 1rem;
			color: var(--ink);
		}
		.column-panel .hint {
			font-size: 0.85rem;
			color: var(--muted);
			margin-bottom: 10px;
		}
		.column-list {
			display: grid;
			gap: 6px;
			max-height: 420px;
			overflow: auto;
		}
		.column-option {
			display: flex;
			align-items: center;
			gap: 8px;
			font-size: 0.9rem;
			color: var(--ink);
		}
		.column-option input {
			margin: 0;
		}
		.utility-grid {
			display: grid;
			grid-template-columns: minmax(0, 1fr) 280px;
			gap: 16px;
			margin-bottom: 20px;
		}
		.utility-panel {
			border: 1px solid var(--border);
			border-radius: 12px;
			background: #f9fafb;
			padding: 12px;
		}
		.utility-panel h2 {
			margin: 0 0 8px;
			font-size: 1rem;
		}
		.utility-panel .hint {
			font-size: 0.85rem;
			color: var(--muted);
			margin-bottom: 10px;
		}
		.history-list {
			display: grid;
			gap: 8px;
		}
		.history-item {
			display: flex;
			gap: 8px;
			align-items: center;
			justify-content: space-between;
			padding: 8px 10px;
			border: 1px solid var(--border);
			border-radius: 8px;
			background: #fff;
		}
		.history-item button {
			width: 100%;
			text-align: left;
			padding: 0;
			background: transparent;
			color: var(--ink);
			border: 0;
		}
		.history-empty {
			font-size: 0.9rem;
			color: var(--muted);
		}
		.history-actions {
			display: flex;
			justify-content: flex-end;
			margin-top: 10px;
		}
		.history-actions button {
			background: #fff;
			color: #b91c1c;
			border: 1px solid #fecaca;
		}
		.history-actions button:hover {
			background: #fef2f2;
		}
		.quick-actions {
			display: flex;
			flex-wrap: wrap;
			gap: 8px;
			margin-bottom: 12px;
		}
		.quick-actions button {
			background: #ffffff;
			color: var(--accent-dark);
			border: 1px solid #99f6e4;
		}
		.quick-actions button:hover {
			background: #f0fdfa;
		}
		.hidden-column {
			display: none;
		}
		table {
			width: 100%;
			border-collapse: collapse;
			font-size: 0.93rem;
			background: #fff;
		}
		th, td {
			border: 1px solid var(--border);
			padding: 8px;
			text-align: left;
			vertical-align: top;
		}
		th { background: #f9fafb; }
		.nowrap { white-space: nowrap; }
	</style>
</head>
<body>
	<div class="container">
		<div class="header">
			<h1>Natural Language Query App</h1>
			<p>Query <strong>data_fusion.db</strong> using plain English prompts.</p>
		</div>
		<div class="content">
			<div class="utility-grid">
				<div>
			<div class="examples">
				Try these prompts or click one to load it into the query box:
				<button type="button" onclick="setPrompt(&quot;Find all Fusion Methods that have been applied to both 'Internet Advertisements' and '17 Category Flower Dataset'&quot;)">
					Find all Fusion Methods that have been applied to both 'Internet Advertisements' and '17 Category Flower Dataset'
				</button>
				<button type="button" onclick="setPrompt(&quot;Find all papers that report U2 (Measurement) uncertainty for a specific sensor type.&quot;)">
					Find all papers that report U2 (Measurement) uncertainty for a specific sensor type.
				</button>
				<button type="button" onclick="setPrompt(&quot;Find the most 'popular' dataset in your graph (the one with the most connections to different methods)&quot;)">
					Find the most 'popular' dataset in your graph (the one with the most connections to different methods)
				</button>
			</div>

					<div class="utility-panel">
						<h2>Random Discovery</h2>
						<div class="hint">Jump to a random paper or dataset and run a search for it.</div>
						<div class="quick-actions">
							<button type="button" onclick="loadRandom('paper')">Random paper</button>
							<button type="button" onclick="loadRandom('dataset')">Random dataset</button>
						</div>
						<div id="random-preview" class="history-empty">No random item loaded yet.</div>
					</div>
				</div>

				<div class="utility-panel">
					<h2>Query History</h2>
					<div class="hint">Recent prompts stay in your browser and can be rerun with one click.</div>
					<div id="history-list" class="history-list"></div>
					<div class="history-actions">
						<button type="button" onclick="clearHistory()">Clear history</button>
					</div>
				</div>
			</div>

			<form method="post" action="/query">
				<textarea name="prompt" placeholder="Enter a natural language query" required>{{ prompt|default('') }}</textarea>
				<button type="submit">Run Query</button>
			</form>

			{% if error %}
				<div class="error">{{ error }}</div>
			{% endif %}

			{% if sql %}
				<details class="sql-block">
					<summary>SQL used</summary>
					<div class="sql-code">{{ sql }}</div>
				</details>
			{% endif %}

			{% if rows is not none %}
				<div class="meta">Rows returned: {{ rows|length }}</div>
				{% if rows|length > 0 %}
					<div class="results-layout">
						<div class="results-table-wrap">
							<table>
								<thead>
									<tr>
										{% for c in columns %}
											<th class="nowrap {% if c not in visible_columns %}hidden-column{% endif %}" data-column="{{ c }}">{{ c }}</th>
										{% endfor %}
									</tr>
								</thead>
								<tbody>
									{% for row in rows %}
										<tr>
											{% for c in columns %}
												<td class="{% if c not in visible_columns %}hidden-column{% endif %}" data-column="{{ c }}">{{ row[c] }}</td>
											{% endfor %}
										</tr>
									{% endfor %}
								</tbody>
							</table>
						</div>
						<div class="column-panel">
							<h2>Columns</h2>
							<div class="hint">Toggle fields on the table. Relevant columns are checked by default.</div>
							<div class="column-list">
								{% for c in columns %}
									<label class="column-option">
										<input type="checkbox" data-column-toggle="{{ c }}" {% if c in visible_columns %}checked{% endif %}>
										<span>{{ c }}</span>
									</label>
								{% endfor %}
							</div>
						</div>
					</div>
				{% endif %}
			{% endif %}
		</div>
	</div>
	<script>
		function setPrompt(value) {
			const promptBox = document.querySelector('textarea[name="prompt"]');
			if (promptBox) {
				promptBox.value = value;
				promptBox.focus();
				promptBox.setSelectionRange(value.length, value.length);
			}
		}

		function applyColumnVisibility(columnName, shouldShow) {
			const hiddenClass = 'hidden-column';
			document.querySelectorAll(`[data-column="${columnName}"]`).forEach((element) => {
				if (shouldShow) {
					element.classList.remove(hiddenClass);
				} else {
					element.classList.add(hiddenClass);
				}
			});
		}

		document.querySelectorAll('[data-column-toggle]').forEach((checkbox) => {
			checkbox.addEventListener('change', () => {
				applyColumnVisibility(checkbox.dataset.columnToggle, checkbox.checked);
			});
		});

			function getHistory() {
				try {
					return JSON.parse(localStorage.getItem('queryHistory') || '[]');
				} catch (error) {
					return [];
				}
			}

			function saveHistory(prompt) {
				const trimmed = (prompt || '').trim();
				if (!trimmed) {
					return;
				}
				const history = getHistory().filter((item) => item !== trimmed);
				history.unshift(trimmed);
				localStorage.setItem('queryHistory', JSON.stringify(history.slice(0, 10)));
			}

			function renderHistory() {
				const list = document.getElementById('history-list');
				if (!list) {
					return;
				}
				const history = getHistory();
				if (history.length === 0) {
					list.innerHTML = '<div class="history-empty">No queries yet.</div>';
					return;
				}
				list.innerHTML = '';
				history.forEach((item) => {
					const wrapper = document.createElement('div');
					wrapper.className = 'history-item';
					const button = document.createElement('button');
					button.type = 'button';
					button.textContent = item;
					button.addEventListener('click', () => setPrompt(item));
					wrapper.appendChild(button);
					list.appendChild(wrapper);
				});
			}

			function clearHistory() {
				localStorage.removeItem('queryHistory');
				renderHistory();
			}

			async function loadRandom(kind) {
				const preview = document.getElementById('random-preview');
				try {
					const response = await fetch(`/api/random?kind=${encodeURIComponent(kind)}`);
					const data = await response.json();
					if (!response.ok || !data.ok) {
						throw new Error(data.error || 'Could not load a random item.');
					}
					if (preview) {
						preview.innerHTML = `<strong>${data.label}</strong><br>${data.summary}`;
					}
					setPrompt(data.prompt);
					const form = document.querySelector('form[action="/query"]');
					if (form) {
						form.requestSubmit ? form.requestSubmit() : form.submit();
					}
				} catch (error) {
					if (preview) {
						preview.textContent = error.message;
					}
				}
			}

			const queryForm = document.querySelector('form[action="/query"]');
			if (queryForm) {
				queryForm.addEventListener('submit', () => {
					const promptBox = document.querySelector('textarea[name="prompt"]');
					if (promptBox) {
						saveHistory(promptBox.value);
					}
				});
			}

			renderHistory();
	</script>
</body>
</html>
"""


def get_conn() -> sqlite3.Connection:
		if not DB_PATH.exists():
				raise FileNotFoundError(f"Database not found: {DB_PATH}")
		conn = sqlite3.connect(DB_PATH)
		conn.row_factory = sqlite3.Row
		return conn


def _extract_quoted_values(prompt: str) -> list[str]:
		single = re.findall(r"'([^']+)'", prompt)
		curly = re.findall(r"‘([^’]+)’", prompt)
		return [v.strip() for v in (single + curly) if v.strip()]


def _tokenize_prompt(prompt: str) -> list[str]:
		stopwords = {
			"a", "an", "and", "any", "are", "be", "been", "both", "by", "for", "from", "find",
			"have", "in", "into", "is", "it", "most", "of", "on", "or", "report", "show",
			"that", "the", "their", "them", "this", "those", "to", "type", "with",
		}
		parts = [part.strip(".,:;!?()[]{}\"\'`).-_") for part in re.split(r"\s+", prompt.lower())]
		return [part for part in parts if part and part not in stopwords and len(part) > 1]


def _looks_like_gibberish(prompt: str, tokens: list[str], quoted_values: list[str]) -> bool:
		if quoted_values:
			return False

		if not tokens:
			return True

		keyword_hints = {
			"paper", "papers", "title", "author", "dataset", "datasets", "data", "method", "methods",
			"fusion", "uncertainty", "sensor", "doi", "abstract", "publisher", "field", "study",
		}
		if any(token in keyword_hints for token in tokens):
			return False

		alpha_tokens = [token for token in tokens if re.search(r"[a-z]", token)]
		if len(alpha_tokens) <= 2:
			vowel_count = sum(1 for token in alpha_tokens if re.search(r"[aeiou]", token))
			return vowel_count == 0

		total_letters = sum(len(re.findall(r"[a-z]", token)) for token in alpha_tokens)
		vowel_letters = sum(len(re.findall(r"[aeiou]", token)) for token in alpha_tokens)
		if total_letters > 0 and vowel_letters / total_letters < 0.18:
			return True

		return False


def _random_row(kind: str) -> dict[str, Any]:
		with get_conn() as conn:
			if kind == "paper":
				row = conn.execute(
					"""
					SELECT DISTINCT
						title,
						author,
						doi,
						publication_title,
						field_of_study
					FROM unified_records
					WHERE title IS NOT NULL AND TRIM(title) <> ''
					ORDER BY RANDOM()
					LIMIT 1
					"""
				).fetchone()
				if row is None:
					raise ValueError("No papers are available.")
				return {"kind": "paper", "title": row["title"], "author": row["author"], "doi": row["doi"], "publication_title": row["publication_title"], "field_of_study": row["field_of_study"], "prompt": f'Show details for paper "{row["title"]}"', "label": "Random paper", "summary": f'{row["title"]} by {row["author"] or "Unknown author"}'}

			if kind == "dataset":
				row = conn.execute(
					"""
					SELECT DISTINCT
						data_name,
						method_name,
						doi,
						data_type,
						collection_method
					FROM unified_records
					WHERE data_name IS NOT NULL AND TRIM(data_name) <> ''
					ORDER BY RANDOM()
					LIMIT 1
					"""
				).fetchone()
				if row is None:
					raise ValueError("No datasets are available.")
				return {"kind": "dataset", "data_name": row["data_name"], "method_name": row["method_name"], "doi": row["doi"], "data_type": row["data_type"], "collection_method": row["collection_method"], "prompt": f'Show details for dataset "{row["data_name"]}"', "label": "Random dataset", "summary": f'{row["data_name"]} via {row["method_name"] or "Unknown method"}'}

			raise ValueError("kind must be paper or dataset")


def _format_random_summary(row: dict[str, Any]) -> str:
		parts = []
		for key in ["title", "author", "data_name", "method_name", "publication_title", "field_of_study", "data_type", "collection_method"]:
			value = row.get(key)
			if value:
				parts.append(f"{key.replace('_', ' ').title()}: {value}")
		return " | ".join(parts)


def _search_sql_for_terms(entity: str, terms: list[str], quoted_values: list[str]) -> tuple[str, list[Any], list[str]]:
		search_columns = [
			"title", "abstract", "keywords", "field_of_study", "publisher", "author", "publication_title", "url",
			"method_name", "method_description", "u1", "u2", "u3",
			"data_name", "data_type", "collection_method", "provenance", "dataset_url",
			"description", "doi", "method_key",
		]
		select_columns = [
			"title", "author", "doi", "publication_title", "field_of_study",
			"method_name", "method_key", "data_name", "data_type",
			"u1", "u2", "u3", "collection_method", "description",
		]

		search_terms = quoted_values or terms
		if not search_terms:
			search_terms = ["*"]

		where_clauses: list[str] = []
		score_clauses: list[str] = []
		score_params: list[Any] = []
		where_params: list[Any] = []

			# Stronger weight for the most informative fields.
		field_weights: dict[str, int] = {
			"title": 6,
			"method_name": 6,
			"data_name": 6,
				"keywords": 5,
			"abstract": 3,
			"method_description": 3,
			"collection_method": 3,
			"description": 3,
			"field_of_study": 2,
			"data_type": 2,
			"publisher": 1,
			"doi": 1,
			"author": 1,
			"publication_title": 1,
			"url": 1,
			"u1": 1,
			"u2": 1,
			"u3": 1,
			"method_key": 1,
			"dataset_url": 1,
				"provenance": 1,
				"sensor": 1,
		}

		for term in search_terms:
			pattern = "%%" if term == "*" else f"%{term}%"
			for column in search_columns:
				weight = field_weights.get(column, 1)
				score_clauses.append(
					f"CASE WHEN LOWER(COALESCE({column}, '')) LIKE LOWER(?) THEN {weight} ELSE 0 END"
				)
				score_params.append(pattern)

		for term in search_terms:
			pattern = "%%" if term == "*" else f"%{term}%"
			term_conditions: list[str] = []
			for column in search_columns:
				term_conditions.append(f"LOWER(COALESCE({column}, '')) LIKE LOWER(?)")
				where_params.append(pattern)
			where_clauses.append("(" + " OR ".join(term_conditions) + ")")

		where_sql = " OR ".join(where_clauses) if where_clauses else "1=1"
		select_sql = ", ".join(select_columns)
		score_sql = " + ".join(score_clauses) if score_clauses else "0"
		sql = f"""
			SELECT
				{select_sql},
				({score_sql}) AS relevance_score
			FROM unified_records
			WHERE {where_sql}
			GROUP BY {select_sql}
			ORDER BY relevance_score DESC, 1 ASC
			LIMIT 100
		"""
		return sql, score_params + where_params, select_columns + ["relevance_score"]


def _suggest_visible_columns(prompt: str, columns: list[str]) -> list[str]:
		normalized = " ".join(prompt.lower().split())
		visible: list[str] = []

		alias_groups = {
			"title": ["paper_title", "title"],
			"paper_title": ["paper_title", "title"],
			"uncertainty_type": ["uncertainty_type", "u2"],
			"u2": ["uncertainty_type", "u2"],
			"data_name": ["data_name", "dataset", "dataset name"],
			"method_name": ["method_name", "method", "methods", "fusion methods"],
			"author": ["author", "authors", "by author"],
			"method_key": ["method_key"],
			"doi": ["doi"],
			"field_of_study": ["field of study", "field_of_study", "topic"],
			"data_type": ["data type", "data_type"],
			"collection_method": ["collection method", "collection_method"],
			"u1": ["u1", "uncertainty", "uncertainty type", "measurement"],
			"u2": ["u2", "uncertainty", "uncertainty type", "measurement"],
			"u3": ["u3", "uncertainty", "uncertainty type", "measurement"],
			"relevance_score": ["relevance", "score", "popular"],
			"matched_datasets": ["applied to both", "both"],
		}

		def has_prompt_match(aliases: list[str]) -> bool:
			return any(alias in normalized for alias in aliases)

		for column in columns:
			if column == "relevance_score":
				continue
			aliases = alias_groups.get(column, [])
			if column in {"title", "paper_title"} and ("paper" in normalized or "title" in normalized):
				visible.append(column)
			elif column == "uncertainty_type" and ("u2" in normalized or "uncertainty" in normalized or "measurement" in normalized):
				visible.append(column)
			elif column == "data_name" and ("dataset" in normalized or "data" in normalized or "popular" in normalized):
				visible.append(column)
			elif column == "method_name" and ("method" in normalized or "fusion" in normalized):
				visible.append(column)
			elif column == "author" and ("author" in normalized or "authors" in normalized or "by author" in normalized):
				visible.append(column)
			elif column in {"u1", "u2", "u3"} and (column in normalized or "uncertainty" in normalized or "measurement" in normalized):
				visible.append(column)
			elif column == "matched_datasets" and "both" in normalized:
				visible.append(column)
			elif column == "distinct_methods" and ("popular" in normalized or "connections" in normalized):
				visible.append(column)
			elif aliases and has_prompt_match(aliases):
				visible.append(column)

		# Prefer the most relevant display fields by default.
		priority_columns = [
			"paper_title", "title", "author", "uncertainty_type", "u1", "u2", "u3", "data_name", "method_name", "matched_datasets", "distinct_methods", "doi"
		]
		for column in priority_columns:
			if column in columns and column not in visible and column != "relevance_score":
				if column in {"paper_title", "title", "author", "uncertainty_type", "u1", "u2", "u3", "data_name", "method_name", "matched_datasets", "distinct_methods", "doi"}:
					visible.append(column)

		if not visible and columns:
			visible.append(columns[0])

		return [column for column in columns if column in visible]


def nl_to_sql(prompt: str) -> tuple[str, list[Any], list[str]]:
		normalized = " ".join(prompt.lower().split())
		quoted = _extract_quoted_values(prompt)
		tokens = _tokenize_prompt(prompt)

		if _looks_like_gibberish(prompt, tokens, quoted):
			raise ValueError("I couldn't understand that prompt. Try mentioning papers, methods, datasets, or U2 uncertainty.")

		if "fusion methods" in normalized and "applied to both" in normalized:
				datasets = quoted[:2] if len(quoted) >= 2 else ["Dataset A", "Dataset B"]
				sql = """
						SELECT
								COALESCE(NULLIF(method_name, ''), method_key) AS fusion_method,
								COUNT(DISTINCT LOWER(TRIM(data_name))) AS matched_datasets
						FROM unified_records
						WHERE data_name IS NOT NULL
							AND method_name IS NOT NULL
							AND LOWER(TRIM(data_name)) IN (LOWER(TRIM(?)), LOWER(TRIM(?)))
						GROUP BY COALESCE(NULLIF(method_name, ''), method_key)
						HAVING COUNT(DISTINCT LOWER(TRIM(data_name))) = 2
						ORDER BY fusion_method
				"""
				return sql, datasets, ["fusion_method", "matched_datasets"]

		if "u2" in normalized and "uncertainty" in normalized:
			quoted = _extract_quoted_values(prompt)
			sensor_type = quoted[0] if quoted else None

			sql = """
				SELECT DISTINCT
					COALESCE(NULLIF(title, ''), '[Unknown title]') AS paper_title,
					doi,
					data_type,
					u2,
					u2 AS uncertainty_type
				FROM unified_records
				WHERE u2 IS NOT NULL
					AND TRIM(u2) <> ''
			"""
			params: list[Any] = []
			if sensor_type:
				sql += """
					AND (
						LOWER(COALESCE(data_type, '')) LIKE LOWER(?)
					 OR LOWER(COALESCE(data_name, '')) LIKE LOWER(?)
					 OR LOWER(COALESCE(collection_method, '')) LIKE LOWER(?)
					)
				"""
				like = f"%{sensor_type}%"
				params.extend([like, like, like])

			sql += " ORDER BY paper_title"
			return sql, params, ["paper_title", "doi", "data_type", "u2", "uncertainty_type"]

		if "most" in normalized and "popular" in normalized and "dataset" in normalized:
				sql = """
						SELECT
								data_name,
								COUNT(DISTINCT COALESCE(NULLIF(method_name, ''), method_key)) AS distinct_methods
						FROM unified_records
						WHERE data_name IS NOT NULL
							AND TRIM(data_name) <> ''
						GROUP BY data_name
						ORDER BY distinct_methods DESC, data_name ASC
						LIMIT 1
				"""
				return sql, [], ["data_name", "distinct_methods"]

		entity = "all"
		if any(word in normalized for word in ["paper", "papers", "article", "articles"]):
			entity = "paper"
		elif any(word in normalized for word in ["method", "methods", "fusion"]):
			entity = "method"
		elif any(word in normalized for word in ["dataset", "datasets", "data"]):
			entity = "dataset"

		sql, params, columns = _search_sql_for_terms(entity, tokens, quoted)
		return sql, params, columns


def execute_prompt(prompt: str) -> tuple[list[str], list[dict[str, Any]], str, list[str]]:
		sql, params, columns = nl_to_sql(prompt)
		with get_conn() as conn:
				rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
		visible_columns = _suggest_visible_columns(prompt, columns)
		return columns, rows, " ".join(sql.split()), visible_columns


@app.get("/")
def home() -> str:
		return render_template_string(PAGE_TEMPLATE, rows=None, columns=[], visible_columns=[], sql="", prompt="", error="")


@app.post("/query")
def query_form() -> str:
		prompt = request.form.get("prompt", "").strip()
		if not prompt:
				return render_template_string(
						PAGE_TEMPLATE,
						rows=None,
						columns=[],
						visible_columns=[],
						sql="",
						prompt="",
						error="Prompt is required.",
				)

		try:
				columns, rows, sql, visible_columns = execute_prompt(prompt)
				return render_template_string(
						PAGE_TEMPLATE,
						rows=rows,
						columns=columns,
						visible_columns=visible_columns,
						sql=sql,
						prompt=prompt,
						error="",
				)
		except Exception as exc:
				return render_template_string(
						PAGE_TEMPLATE,
						rows=None,
						columns=[],
						visible_columns=[],
						sql="",
						prompt=prompt,
						error=str(exc),
				)


@app.post("/api/query")
def query_api() -> Any:
		payload = request.get_json(silent=True) or {}
		prompt = str(payload.get("prompt", "")).strip()
		if not prompt:
				return jsonify({"ok": False, "error": "prompt is required"}), 400

		try:
				columns, rows, sql, visible_columns = execute_prompt(prompt)
				return jsonify({"ok": True, "columns": columns, "visible_columns": visible_columns, "rows": rows, "sql": sql})
		except Exception as exc:
				return jsonify({"ok": False, "error": str(exc)}), 400


@app.get("/api/random")
def random_item() -> Any:
		kind = str(request.args.get("kind", "")).strip().lower()
		if kind not in {"paper", "dataset"}:
			return jsonify({"ok": False, "error": "kind must be paper or dataset"}), 400

		try:
			row = _random_row(kind)
			summary = row.get("summary") or _format_random_summary(row)
			return jsonify(
				{
					"ok": True,
					"kind": kind,
					"label": row.get("label", "Random item"),
					"summary": summary,
					"prompt": row.get("prompt", ""),
					"item": row,
				}
			)
		except Exception as exc:
			return jsonify({"ok": False, "error": str(exc)}), 400


if __name__ == "__main__":
		app.run(host="127.0.0.1", port=5000, debug=True)
