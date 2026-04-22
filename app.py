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


FTS_TABLE = "unified_records_fts"
FTS_COLUMNS = [
	"title", "abstract", "keywords", "field_of_study", "publisher", "author", "publication_title", "url",
	"method_name", "method_description", "u1", "u2", "u3",
	"data_name", "data_type", "collection_method", "provenance", "dataset_url",
	"description", "doi", "method_key",
]
RESULT_COLUMNS = [
	"title", "author", "doi", "publication_title", "field_of_study",
	"method_name", "method_key", "data_name", "data_type",
	"u1", "u2", "u3", "collection_method", "description",
]
FTS_INITIALIZED = False


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
			--body-grad-1: #d1fae5;
			--body-grad-2: #bfdbfe;
			--card: #ffffff;
			--ink: #111827;
			--muted: #4b5563;
			--accent: #0d9488;
			--accent-dark: #0f766e;
			--border: #d1d5db;
			--surface-soft: #f9fafb;
			--surface-bright: #ffffff;
			--surface-info: #ecfeff;
			--border-info: #99f6e4;
			--ink-info: #134e4a;
			--history-danger-bg: #fef2f2;
			--history-danger-border: #fecaca;
			--history-danger-ink: #b91c1c;
			--sql-ink: #1f2937;
			--shadow: rgba(17, 24, 39, 0.08);
			--error-bg: #fee2e2;
			--error-ink: #991b1b;
		}
		[data-theme="dark"] {
			--bg: #0b1220;
			--body-grad-1: #134e4a;
			--body-grad-2: #1e3a8a;
			--card: #111827;
			--ink: #e5e7eb;
			--muted: #9ca3af;
			--accent: #14b8a6;
			--accent-dark: #0d9488;
			--border: #334155;
			--surface-soft: #1f2937;
			--surface-bright: #111827;
			--surface-info: #122b33;
			--border-info: #1f766f;
			--ink-info: #a7f3d0;
			--history-danger-bg: #3f1d1d;
			--history-danger-border: #7f1d1d;
			--history-danger-ink: #fca5a5;
			--sql-ink: #d1d5db;
			--shadow: rgba(2, 6, 23, 0.45);
			--error-bg: #3f1d1d;
			--error-ink: #fca5a5;
		}
		* { box-sizing: border-box; }
		body {
			margin: 0;
			font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
			color: var(--ink);
			background:
				radial-gradient(circle at 20% 10%, var(--body-grad-1) 0%, transparent 40%),
				radial-gradient(circle at 80% 90%, var(--body-grad-2) 0%, transparent 35%),
				var(--bg);
			min-height: 100vh;
			padding: 24px;
			transition: background 0.25s ease, color 0.25s ease;
		}
		.container {
			max-width: 980px;
			margin: 0 auto;
			background: var(--card);
			border: 1px solid var(--border);
			border-radius: 16px;
			box-shadow: 0 12px 30px var(--shadow);
			overflow: hidden;
			transition: background 0.25s ease, border-color 0.25s ease, box-shadow 0.25s ease;
		}
		.header {
			padding: 24px;
			background: linear-gradient(130deg, var(--accent), var(--accent-dark));
			color: #ffffff;
			display: flex;
			align-items: flex-start;
			justify-content: space-between;
			gap: 12px;
			transition: background 0.25s ease;
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
		.theme-toggle {
			background: rgba(255, 255, 255, 0.18);
			border: 1px solid rgba(255, 255, 255, 0.4);
			color: #ffffff;
			padding: 8px 12px;
			border-radius: 999px;
			font-size: 0.85rem;
			font-weight: 600;
		}
		.theme-toggle:hover {
			background: rgba(255, 255, 255, 0.28);
		}
		.examples {
			margin: 0 0 20px;
			padding: 12px;
			background: var(--surface-info);
			border: 1px solid var(--border-info);
			border-radius: 10px;
			color: var(--ink-info);
			line-height: 1.45;
		}
		.examples code {
			display: block;
			white-space: pre-wrap;
			margin: 6px 0;
			background: color-mix(in srgb, var(--surface-bright) 85%, transparent);
			border-radius: 8px;
			padding: 8px;
		}
		.examples button {
			display: block;
			width: 100%;
			text-align: left;
			margin: 8px 0 0;
			background: var(--surface-bright);
			color: var(--ink-info);
			border: 1px solid var(--border-info);
			border-radius: 8px;
			padding: 10px 12px;
			cursor: pointer;
		}
		.examples button:hover {
			background: color-mix(in srgb, var(--surface-info) 70%, var(--surface-bright));
		}
		.query-canvas {
			margin: 0 0 20px;
			padding: 14px;
			background: linear-gradient(
				140deg,
				color-mix(in srgb, var(--surface-info) 80%, var(--surface-bright)),
				color-mix(in srgb, var(--surface-soft) 84%, var(--surface-bright))
			);
			border: 1px solid var(--border-info);
			border-radius: 12px;
		}
		.query-canvas h2 {
			margin: 0 0 8px;
			font-size: 1rem;
			color: var(--ink);
		}
		.query-canvas .hint {
			font-size: 0.86rem;
			color: var(--muted);
			margin-bottom: 10px;
		}
		.canvas-chips {
			display: flex;
			flex-wrap: wrap;
			gap: 8px;
			margin-bottom: 12px;
		}
		.canvas-chip {
			background: var(--surface-bright);
			color: var(--ink);
			border: 1px solid var(--border);
			border-radius: 999px;
			padding: 6px 10px;
			font-size: 0.82rem;
			cursor: grab;
			user-select: none;
		}
		.canvas-chip:active {
			cursor: grabbing;
		}
		.canvas-chip.is-selected {
			border-color: var(--accent);
			box-shadow: 0 0 0 2px color-mix(in srgb, var(--accent) 25%, transparent);
		}
		.canvas-grid {
			display: grid;
			grid-template-columns: repeat(2, minmax(0, 1fr));
			gap: 8px;
		}
		.canvas-slot {
			background: var(--surface-bright);
			border: 1px dashed var(--border);
			border-radius: 10px;
			padding: 10px;
			min-height: 62px;
			display: grid;
			gap: 6px;
		}
		.canvas-slot strong {
			font-size: 0.8rem;
			color: var(--muted);
			text-transform: uppercase;
			letter-spacing: 0.04em;
		}
		.canvas-slot-value {
			font-size: 0.9rem;
			color: var(--ink);
		}
		.canvas-slot.is-active {
			border-color: var(--accent);
			background: color-mix(in srgb, var(--surface-info) 68%, var(--surface-bright));
		}
		.canvas-preview {
			margin-top: 10px;
			padding: 10px;
			border-radius: 10px;
			border: 1px solid var(--border);
			background: var(--surface-bright);
			font-size: 0.9rem;
			line-height: 1.45;
			color: var(--ink);
			min-height: 46px;
		}
		.canvas-actions {
			display: flex;
			flex-wrap: wrap;
			gap: 8px;
			margin-top: 10px;
		}
		.canvas-actions .secondary {
			background: var(--surface-bright);
			color: var(--ink);
			border: 1px solid var(--border);
		}
		.canvas-actions .secondary:hover {
			background: var(--surface-soft);
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
			background: var(--surface-soft);
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
			color: var(--sql-ink);
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
			background: var(--surface-soft);
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
		.side-stack {
			display: grid;
			gap: 12px;
			align-content: start;
		}
		.utility-panel {
			border: 1px solid var(--border);
			border-radius: 12px;
			background: var(--surface-soft);
			padding: 12px;
		}
		.utility-panel h2 {
			margin: 0 0 8px;
			font-size: 1rem;
		}
		.theme-editor-controls {
			display: grid;
			gap: 10px;
		}
		.theme-editor-row {
			display: flex;
			align-items: center;
			justify-content: space-between;
			gap: 10px;
		}
		.theme-editor-row label {
			font-size: 0.9rem;
			font-weight: 600;
			color: var(--ink);
		}
		#main-color-input {
			width: 44px;
			height: 34px;
			padding: 0;
			border: 1px solid var(--border);
			border-radius: 8px;
			background: var(--surface-bright);
			cursor: pointer;
		}
		#main-color-hex {
			font-size: 0.85rem;
			font-family: Consolas, "Liberation Mono", Menlo, monospace;
			color: var(--muted);
		}
		.theme-actions {
			display: flex;
			gap: 8px;
			flex-wrap: wrap;
		}
		.theme-actions .secondary {
			background: var(--surface-bright);
			color: var(--ink);
			border: 1px solid var(--border);
		}
		.theme-actions .secondary:hover {
			background: var(--surface-soft);
		}
		.theme-swatches {
			display: flex;
			gap: 8px;
			padding-top: 2px;
		}
		.theme-swatch {
			width: 22px;
			height: 22px;
			border-radius: 50%;
			border: 1px solid var(--border);
		}
		.theme-swatch.main {
			background: var(--accent);
		}
		.theme-swatch.alt {
			background: var(--accent-dark);
		}
		.theme-swatch.soft {
			background: var(--surface-info);
		}
		.utility-panel .hint {
			font-size: 0.85rem;
			color: var(--muted);
			margin-bottom: 10px;
		}
		.history-list {
			display: grid;
			gap: 8px;
			max-height: 320px;
			overflow-y: auto;
			padding-right: 4px;
		}
		.history-item {
			display: flex;
			gap: 8px;
			align-items: center;
			justify-content: space-between;
			padding: 8px 10px;
			border: 1px solid var(--border);
			border-radius: 8px;
			background: var(--surface-bright);
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
			background: var(--surface-bright);
			color: var(--history-danger-ink);
			border: 1px solid var(--history-danger-border);
		}
		.history-actions button:hover {
			background: var(--history-danger-bg);
		}
		.quick-actions {
			display: flex;
			flex-wrap: wrap;
			gap: 8px;
			margin-bottom: 12px;
		}
		.quick-actions button {
			background: var(--surface-bright);
			color: var(--accent-dark);
			border: 1px solid var(--border-info);
		}
		.quick-actions button:hover {
			background: color-mix(in srgb, var(--surface-info) 65%, var(--surface-bright));
		}
		.ttt-panel {
			display: grid;
			gap: 10px;
		}
		.ttt-status {
			font-size: 0.88rem;
			color: var(--muted);
		}
		.ttt-score-grid {
			display: grid;
			grid-template-columns: repeat(3, minmax(0, 1fr));
			gap: 6px;
		}
		.ttt-score-card {
			border: 1px solid var(--border);
			border-radius: 8px;
			background: var(--surface-bright);
			padding: 6px 8px;
			text-align: center;
		}
		.ttt-score-label {
			font-size: 0.72rem;
			color: var(--muted);
			text-transform: uppercase;
			letter-spacing: 0.04em;
		}
		.ttt-score-value {
			margin-top: 2px;
			font-size: 1rem;
			font-weight: 700;
			color: var(--ink);
		}
		.ttt-board {
			display: grid;
			grid-template-columns: repeat(3, minmax(0, 1fr));
			gap: 6px;
		}
		.ttt-cell {
			width: 100%;
			aspect-ratio: 1;
			border-radius: 8px;
			border: 1px solid var(--border);
			background: var(--surface-bright);
			color: var(--ink);
			font-size: 1.15rem;
			font-weight: 700;
			line-height: 1;
			padding: 0;
		}
		.ttt-cell:hover {
			background: var(--surface-soft);
		}
		.ttt-cell:disabled {
			cursor: not-allowed;
			opacity: 0.95;
		}
		.ttt-actions {
			display: flex;
			justify-content: flex-end;
		}
		.ttt-actions button {
			background: var(--surface-bright);
			color: var(--ink);
			border: 1px solid var(--border);
		}
		.ttt-actions button:hover {
			background: var(--surface-soft);
		}
		.theme-drawer-tab {
			position: fixed;
			right: 0;
			top: 50%;
			transform: translateY(-50%);
			z-index: 35;
			border-radius: 12px 0 0 12px;
			padding: 12px 14px;
			box-shadow: 0 10px 24px var(--shadow);
		}
		.theme-drawer-backdrop {
			position: fixed;
			inset: 0;
			background: rgba(2, 6, 23, 0.45);
			opacity: 0;
			pointer-events: none;
			transition: opacity 0.2s ease;
			z-index: 40;
		}
		.theme-drawer {
			position: fixed;
			top: 0;
			right: 0;
			height: 100vh;
			width: min(360px, 92vw);
			background: var(--card);
			border-left: 1px solid var(--border);
			padding: 18px;
			transform: translateX(102%);
			transition: transform 0.25s ease;
			z-index: 45;
			overflow-y: auto;
			box-shadow: -12px 0 32px rgba(2, 6, 23, 0.26);
		}
		.theme-drawer-head {
			display: flex;
			align-items: center;
			justify-content: space-between;
			gap: 8px;
			margin-bottom: 8px;
		}
		.theme-drawer-close {
			background: var(--surface-bright);
			border: 1px solid var(--border);
			color: var(--ink);
			padding: 6px 10px;
		}
		.theme-drawer-close:hover {
			background: var(--surface-soft);
		}
		body.theme-drawer-open .theme-drawer-backdrop {
			opacity: 1;
			pointer-events: auto;
		}
		body.theme-drawer-open .theme-drawer {
			transform: translateX(0);
		}
		.hidden-column {
			display: none;
		}
		table {
			width: 100%;
			border-collapse: collapse;
			font-size: 0.93rem;
			background: var(--surface-bright);
		}
		th, td {
			border: 1px solid var(--border);
			padding: 8px;
			text-align: left;
			vertical-align: top;
		}
		th { background: var(--surface-soft); }
		.nowrap { white-space: nowrap; }
		@media (max-width: 760px) {
			.header {
				flex-direction: column;
				align-items: flex-start;
			}
			.theme-toggle {
				align-self: flex-end;
			}
			.theme-drawer-tab {
				top: auto;
				bottom: 16px;
				transform: none;
			}
			.canvas-grid {
				grid-template-columns: 1fr;
			}
		}
	</style>
</head>
<body>
	<div class="container">
		<div class="header">
			<div>
				<h1>Natural Language Query App</h1>
				<p>Query <strong>data_fusion.db</strong> using plain English prompts.</p>
			</div>
			<button type="button" class="theme-toggle" id="theme-toggle" aria-label="Toggle dark mode">
				Toggle Dark Mode
			</button>
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

					<div class="query-canvas">
						<h2>Query Canvas</h2>
						<div class="hint">Drag a chip into each lane or click a chip and then click a lane to assemble a query.</div>
						<div class="canvas-chips" id="canvas-chip-pool">
							<button type="button" class="canvas-chip" data-canvas-token="Show">Show</button>
							<button type="button" class="canvas-chip" data-canvas-token="Find">Find</button>
							<button type="button" class="canvas-chip" data-canvas-token="Compare">Compare</button>
							<button type="button" class="canvas-chip" data-canvas-token="the most connected">the most connected</button>
							<button type="button" class="canvas-chip" data-canvas-token="papers">papers</button>
							<button type="button" class="canvas-chip" data-canvas-token="datasets">datasets</button>
							<button type="button" class="canvas-chip" data-canvas-token="methods">methods</button>
							<button type="button" class="canvas-chip" data-canvas-token="uncertainty records">uncertainty records</button>
							<button type="button" class="canvas-chip" data-canvas-token="with U2 uncertainty">with U2 uncertainty</button>
							<button type="button" class="canvas-chip" data-canvas-token="for a sensor type">for a sensor type</button>
							<button type="button" class="canvas-chip" data-canvas-token="for a specific dataset">for a specific dataset</button>
							<button type="button" class="canvas-chip" data-canvas-token="used in multiple papers">used in multiple papers</button>
							<button type="button" class="canvas-chip" data-canvas-token="and include title, doi, method_name">and include title, doi, method_name</button>
							<button type="button" class="canvas-chip" data-canvas-token="and rank by relevance">and rank by relevance</button>
							<button type="button" class="canvas-chip" data-canvas-token="and group similar methods">and group similar methods</button>
						</div>
						<div class="canvas-grid" id="query-canvas-grid">
							<div class="canvas-slot" data-canvas-slot="intent">
								<strong>Intent</strong>
								<div class="canvas-slot-value">Drop action chip</div>
							</div>
							<div class="canvas-slot" data-canvas-slot="focus">
								<strong>Focus</strong>
								<div class="canvas-slot-value">Drop subject chip</div>
							</div>
							<div class="canvas-slot" data-canvas-slot="constraint">
								<strong>Constraint</strong>
								<div class="canvas-slot-value">Drop filter chip</div>
							</div>
							<div class="canvas-slot" data-canvas-slot="detail">
								<strong>Detail</strong>
								<div class="canvas-slot-value">Drop detail chip</div>
							</div>
						</div>
						<div class="canvas-preview" id="canvas-preview">Your generated query will appear here.</div>
						<div class="canvas-actions">
							<button type="button" id="canvas-use-prompt">Use In Query Box</button>
							<button type="button" id="canvas-run-query">Run Canvas Query</button>
							<button type="button" id="canvas-surprise" class="secondary">Surprise Me</button>
							<button type="button" id="canvas-clear" class="secondary">Clear Canvas</button>
						</div>
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

				<div class="side-stack">
					<div class="utility-panel">
						<h2>Query History</h2>
						<div class="hint">Recent prompts stay in your browser and can be rerun with one click.</div>
						<div id="history-list" class="history-list"></div>
						<div class="history-actions">
							<button type="button" onclick="clearHistory()">Clear history</button>
						</div>
					</div>
					<div class="utility-panel ttt-panel">
						<h2>Tic-Tac-Toe</h2>
						<div class="hint">You are X. Bot is O and plays random open spots.</div>
						<div class="ttt-score-grid" aria-label="Tic-Tac-Toe score tracker">
							<div class="ttt-score-card">
								<div class="ttt-score-label">You</div>
								<div class="ttt-score-value" id="ttt-score-you">0</div>
							</div>
							<div class="ttt-score-card">
								<div class="ttt-score-label">Bot</div>
								<div class="ttt-score-value" id="ttt-score-bot">0</div>
							</div>
							<div class="ttt-score-card">
								<div class="ttt-score-label">Draws</div>
								<div class="ttt-score-value" id="ttt-score-draw">0</div>
							</div>
						</div>
						<div id="ttt-status" class="ttt-status">Your turn.</div>
						<div id="ttt-board" class="ttt-board" role="grid" aria-label="Tic-Tac-Toe board">
							<button type="button" class="ttt-cell" data-ttt-index="0" aria-label="Cell 1"></button>
							<button type="button" class="ttt-cell" data-ttt-index="1" aria-label="Cell 2"></button>
							<button type="button" class="ttt-cell" data-ttt-index="2" aria-label="Cell 3"></button>
							<button type="button" class="ttt-cell" data-ttt-index="3" aria-label="Cell 4"></button>
							<button type="button" class="ttt-cell" data-ttt-index="4" aria-label="Cell 5"></button>
							<button type="button" class="ttt-cell" data-ttt-index="5" aria-label="Cell 6"></button>
							<button type="button" class="ttt-cell" data-ttt-index="6" aria-label="Cell 7"></button>
							<button type="button" class="ttt-cell" data-ttt-index="7" aria-label="Cell 8"></button>
							<button type="button" class="ttt-cell" data-ttt-index="8" aria-label="Cell 9"></button>
						</div>
						<div class="ttt-actions">
							<button type="button" id="ttt-reset-scores">Reset scores</button>
							<button type="button" id="ttt-reset">New game</button>
						</div>
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
	<button type="button" class="theme-drawer-tab" id="theme-editor-open" aria-label="Open theme editor">
		Theme Editor
	</button>
	<div class="theme-drawer-backdrop" id="theme-editor-backdrop"></div>
	<aside class="theme-drawer" id="theme-editor-drawer" aria-hidden="true" aria-label="Theme editor panel">
		<div class="theme-drawer-head">
			<h2>Theme Editor</h2>
			<button type="button" class="theme-drawer-close" id="theme-editor-close" aria-label="Close theme editor">Close</button>
		</div>
		<div class="hint">Pick a main color and the app auto-builds a matching palette.</div>
		<div class="theme-editor-controls">
			<div class="theme-editor-row">
				<label for="main-color-input">Main color</label>
				<input type="color" id="main-color-input" value="#0d9488">
			</div>
			<div id="main-color-hex">#0D9488</div>
			<div class="theme-actions">
				<button type="button" id="apply-theme-color">Apply color</button>
				<button type="button" id="reset-theme-color" class="secondary">Reset</button>
			</div>
			<div class="theme-swatches" aria-hidden="true">
				<span class="theme-swatch main" title="Main accent"></span>
				<span class="theme-swatch alt" title="Secondary accent"></span>
				<span class="theme-swatch soft" title="Soft surface"></span>
			</div>
		</div>
	</aside>
	<script>
		const THEME_KEY = 'themePreference';
		const THEME_MAIN_COLOR_KEY = 'themeMainColor';
		const DEFAULT_MAIN_COLOR = '#0d9488';

		function clamp(value, min, max) {
			return Math.min(max, Math.max(min, value));
		}

		function normalizeHexColor(value) {
			if (typeof value !== 'string') {
				return null;
			}
			const trimmed = value.trim();
			if (!/^#([0-9a-fA-F]{6})$/.test(trimmed)) {
				return null;
			}
			return trimmed.toLowerCase();
		}

		function shiftHue(hue, amount) {
			return (hue + amount + 360) % 360;
		}

		function hexToRgb(hex) {
			const normalized = normalizeHexColor(hex);
			if (!normalized) {
				return null;
			}
			const value = normalized.slice(1);
			return {
				r: Number.parseInt(value.slice(0, 2), 16),
				g: Number.parseInt(value.slice(2, 4), 16),
				b: Number.parseInt(value.slice(4, 6), 16),
			};
		}

		function rgbToHsl(r, g, b) {
			const red = r / 255;
			const green = g / 255;
			const blue = b / 255;
			const max = Math.max(red, green, blue);
			const min = Math.min(red, green, blue);
			let h = 0;
			let s = 0;
			const l = (max + min) / 2;

			if (max !== min) {
				const d = max - min;
				s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
				switch (max) {
					case red:
						h = (green - blue) / d + (green < blue ? 6 : 0);
						break;
					case green:
						h = (blue - red) / d + 2;
						break;
					default:
						h = (red - green) / d + 4;
						break;
				}
				h /= 6;
			}

			return {
				h: Math.round(h * 360),
				s: Math.round(s * 100),
				l: Math.round(l * 100),
			};
		}

		function hslToHex(h, s, l) {
			const hue = ((h % 360) + 360) % 360;
			const sat = clamp(s, 0, 100) / 100;
			const light = clamp(l, 0, 100) / 100;

			const c = (1 - Math.abs(2 * light - 1)) * sat;
			const x = c * (1 - Math.abs((hue / 60) % 2 - 1));
			const m = light - c / 2;

			let rPrime = 0;
			let gPrime = 0;
			let bPrime = 0;

			if (hue < 60) {
				rPrime = c;
				gPrime = x;
			} else if (hue < 120) {
				rPrime = x;
				gPrime = c;
			} else if (hue < 180) {
				gPrime = c;
				bPrime = x;
			} else if (hue < 240) {
				gPrime = x;
				bPrime = c;
			} else if (hue < 300) {
				rPrime = x;
				bPrime = c;
			} else {
				rPrime = c;
				bPrime = x;
			}

			const r = Math.round((rPrime + m) * 255);
			const g = Math.round((gPrime + m) * 255);
			const b = Math.round((bPrime + m) * 255);
			return `#${[r, g, b].map((n) => n.toString(16).padStart(2, '0')).join('')}`;
		}

		function getStoredMainColor() {
			return normalizeHexColor(localStorage.getItem(THEME_MAIN_COLOR_KEY)) || DEFAULT_MAIN_COLOR;
		}

		function buildPaletteFromMainColor(mainColor, theme) {
			const rgb = hexToRgb(mainColor);
			if (!rgb) {
				return null;
			}

			const { h, s } = rgbToHsl(rgb.r, rgb.g, rgb.b);
			const baseSat = clamp(Math.max(s, 56), 40, 88);

			if (theme === 'dark') {
				const accent = hslToHex(h, baseSat, 58);
				const accentDark = hslToHex(h, clamp(baseSat + 3, 40, 92), 47);
				return {
					'--bg': hslToHex(shiftHue(h, 220), 34, 10),
					'--body-grad-1': hslToHex(shiftHue(h, 20), clamp(baseSat * 0.55, 30, 72), 24),
					'--body-grad-2': hslToHex(shiftHue(h, 220), clamp(baseSat * 0.62, 34, 78), 27),
					'--card': hslToHex(shiftHue(h, 220), 26, 14),
					'--ink': hslToHex(shiftHue(h, 208), 16, 92),
					'--muted': hslToHex(shiftHue(h, 208), 10, 68),
					'--accent': accent,
					'--accent-dark': accentDark,
					'--border': hslToHex(shiftHue(h, 215), 16, 30),
					'--surface-soft': hslToHex(shiftHue(h, 215), 20, 20),
					'--surface-bright': hslToHex(shiftHue(h, 220), 22, 15),
					'--surface-info': hslToHex(shiftHue(h, 12), 30, 20),
					'--border-info': hslToHex(shiftHue(h, 12), 35, 36),
					'--ink-info': hslToHex(shiftHue(h, 12), 58, 80),
					'--history-danger-bg': '#3f1d1d',
					'--history-danger-border': '#7f1d1d',
					'--history-danger-ink': '#fca5a5',
					'--sql-ink': hslToHex(shiftHue(h, 210), 12, 84),
					'--shadow': 'rgba(2, 6, 23, 0.45)',
					'--error-bg': '#3f1d1d',
					'--error-ink': '#fca5a5',
				};
			}

			const accent = hslToHex(h, baseSat, 42);
			const accentDark = hslToHex(h, clamp(baseSat + 5, 45, 92), 30);
			return {
				'--bg': hslToHex(shiftHue(h, 220), 35, 96),
				'--body-grad-1': hslToHex(shiftHue(h, 30), clamp(baseSat * 0.55, 28, 70), 88),
				'--body-grad-2': hslToHex(shiftHue(h, 220), clamp(baseSat * 0.52, 24, 68), 86),
				'--card': '#ffffff',
				'--ink': hslToHex(shiftHue(h, 215), 24, 13),
				'--muted': hslToHex(shiftHue(h, 215), 14, 35),
				'--accent': accent,
				'--accent-dark': accentDark,
				'--border': hslToHex(shiftHue(h, 215), 18, 82),
				'--surface-soft': hslToHex(shiftHue(h, 215), 18, 97),
				'--surface-bright': '#ffffff',
				'--surface-info': hslToHex(shiftHue(h, 12), clamp(baseSat * 0.45, 24, 58), 94),
				'--border-info': hslToHex(shiftHue(h, 12), clamp(baseSat * 0.55, 28, 68), 74),
				'--ink-info': hslToHex(shiftHue(h, 12), clamp(baseSat, 40, 80), 24),
				'--history-danger-bg': '#fef2f2',
				'--history-danger-border': '#fecaca',
				'--history-danger-ink': '#b91c1c',
				'--sql-ink': hslToHex(shiftHue(h, 215), 22, 18),
				'--shadow': 'rgba(17, 24, 39, 0.08)',
				'--error-bg': '#fee2e2',
				'--error-ink': '#991b1b',
			};
		}

		function applyPaletteVariables(variables) {
			if (!variables) {
				return;
			}
			Object.entries(variables).forEach(([key, value]) => {
				document.documentElement.style.setProperty(key, String(value));
			});
		}

		function setMainColorLabel(color) {
			const label = document.getElementById('main-color-hex');
			if (label) {
				label.textContent = color.toUpperCase();
			}
		}

		function applyThemeColor(mainColor, persist) {
			const normalized = normalizeHexColor(mainColor) || DEFAULT_MAIN_COLOR;
			const activeTheme = document.documentElement.getAttribute('data-theme') || getPreferredTheme();
			applyPaletteVariables(buildPaletteFromMainColor(normalized, activeTheme));
			setMainColorLabel(normalized);

			const picker = document.getElementById('main-color-input');
			if (picker) {
				picker.value = normalized;
			}

			if (persist) {
				localStorage.setItem(THEME_MAIN_COLOR_KEY, normalized);
			}
		}

		function getPreferredTheme() {
			const savedTheme = localStorage.getItem(THEME_KEY);
			if (savedTheme === 'dark' || savedTheme === 'light') {
				return savedTheme;
			}
			return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
		}

		function setTheme(theme) {
			document.documentElement.setAttribute('data-theme', theme);
			localStorage.setItem(THEME_KEY, theme);
			applyThemeColor(getStoredMainColor(), false);
			const toggle = document.getElementById('theme-toggle');
			if (toggle) {
				toggle.textContent = theme === 'dark' ? 'Switch to Light Mode' : 'Switch to Dark Mode';
				toggle.setAttribute('aria-pressed', theme === 'dark' ? 'true' : 'false');
			}
		}

		function toggleTheme() {
			const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
			setTheme(currentTheme === 'dark' ? 'light' : 'dark');
		}

		function setThemeEditorOpen(isOpen) {
			document.body.classList.toggle('theme-drawer-open', isOpen);
			const drawer = document.getElementById('theme-editor-drawer');
			if (drawer) {
				drawer.setAttribute('aria-hidden', isOpen ? 'false' : 'true');
			}
		}

		setTheme(getPreferredTheme());

		function setPrompt(value) {
			const promptBox = document.querySelector('textarea[name="prompt"]');
			if (promptBox) {
				promptBox.value = value;
				promptBox.focus();
				promptBox.setSelectionRange(value.length, value.length);
			}
		}

		const canvasState = {
			intent: '',
			focus: '',
			constraint: '',
			detail: '',
		};
		let selectedCanvasToken = '';

		function buildCanvasPrompt() {
			const parts = [canvasState.intent, canvasState.focus, canvasState.constraint, canvasState.detail]
				.map((part) => (part || '').trim())
				.filter(Boolean);
			if (parts.length === 0) {
				return '';
			}
			const sentence = parts.join(' ').replace(/\\s+/g, ' ').trim();
			return /[.?!]$/.test(sentence) ? sentence : `${sentence}.`;
		}

		function renderCanvasPreview() {
			const preview = document.getElementById('canvas-preview');
			if (!preview) {
				return;
			}
			const prompt = buildCanvasPrompt();
			preview.textContent = prompt || 'Your generated query will appear here.';
		}

		function setCanvasSlotValue(slotName, value) {
			canvasState[slotName] = value;
			const slot = document.querySelector(`[data-canvas-slot="${slotName}"]`);
			if (slot) {
				const valueNode = slot.querySelector('.canvas-slot-value');
				if (valueNode) {
					valueNode.textContent = value || `Drop ${slotName} chip`;
				}
				slot.classList.toggle('is-active', Boolean(value));
			}
			renderCanvasPreview();
		}

		function clearCanvasSelections() {
			setCanvasSlotValue('intent', '');
			setCanvasSlotValue('focus', '');
			setCanvasSlotValue('constraint', '');
			setCanvasSlotValue('detail', '');
		}

		function setupCanvasInteractions() {
			const chips = document.querySelectorAll('.canvas-chip');
			chips.forEach((chip) => {
				chip.setAttribute('draggable', 'true');
				chip.addEventListener('dragstart', (event) => {
					const token = chip.dataset.canvasToken || '';
					event.dataTransfer.setData('text/plain', token);
					event.dataTransfer.effectAllowed = 'copy';
				});
				chip.addEventListener('click', () => {
					selectedCanvasToken = chip.dataset.canvasToken || '';
					document.querySelectorAll('.canvas-chip').forEach((item) => item.classList.remove('is-selected'));
					chip.classList.add('is-selected');
				});
			});

			document.querySelectorAll('[data-canvas-slot]').forEach((slot) => {
				slot.addEventListener('dragover', (event) => {
					event.preventDefault();
					slot.classList.add('is-active');
				});
				slot.addEventListener('dragleave', () => {
					if (!(canvasState[slot.dataset.canvasSlot || ''])) {
						slot.classList.remove('is-active');
					}
				});
				slot.addEventListener('drop', (event) => {
					event.preventDefault();
					const token = event.dataTransfer.getData('text/plain');
					const slotName = slot.dataset.canvasSlot || '';
					if (slotName) {
						setCanvasSlotValue(slotName, token);
					}
				});
				slot.addEventListener('click', () => {
					if (!selectedCanvasToken) {
						return;
					}
					const slotName = slot.dataset.canvasSlot || '';
					if (slotName) {
						setCanvasSlotValue(slotName, selectedCanvasToken);
					}
				});
			});

			const useButton = document.getElementById('canvas-use-prompt');
			if (useButton) {
				useButton.addEventListener('click', () => {
					const prompt = buildCanvasPrompt();
					if (prompt) {
						setPrompt(prompt);
					}
				});
			}

			const runButton = document.getElementById('canvas-run-query');
			if (runButton) {
				runButton.addEventListener('click', () => {
					const prompt = buildCanvasPrompt();
					if (!prompt) {
						return;
					}
					setPrompt(prompt);
					const form = document.querySelector('form[action="/query"]');
					if (form) {
						form.requestSubmit ? form.requestSubmit() : form.submit();
					}
				});
			}

			const surpriseButton = document.getElementById('canvas-surprise');
			if (surpriseButton) {
				surpriseButton.addEventListener('click', () => {
					const options = {
						intent: ['Show', 'Find', 'Compare', 'Find'],
						focus: ['papers', 'datasets', 'methods', 'uncertainty records'],
						constraint: ['with U2 uncertainty', 'for a sensor type', 'for a specific dataset', 'used in multiple papers'],
						detail: ['and include title, doi, method_name', 'and rank by relevance', 'and group similar methods', ''],
					};
					Object.entries(options).forEach(([slotName, choices]) => {
						const pick = choices[Math.floor(Math.random() * choices.length)] || '';
						setCanvasSlotValue(slotName, pick);
					});
				});
			}

			const clearButton = document.getElementById('canvas-clear');
			if (clearButton) {
				clearButton.addEventListener('click', () => {
					selectedCanvasToken = '';
					document.querySelectorAll('.canvas-chip').forEach((item) => item.classList.remove('is-selected'));
					clearCanvasSelections();
				});
			}

			renderCanvasPreview();
		}

		setupCanvasInteractions();

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

		const themeToggle = document.getElementById('theme-toggle');
		if (themeToggle) {
			themeToggle.addEventListener('click', toggleTheme);
		}

		const themeEditorOpenButton = document.getElementById('theme-editor-open');
		if (themeEditorOpenButton) {
			themeEditorOpenButton.addEventListener('click', () => setThemeEditorOpen(true));
		}

		const themeEditorCloseButton = document.getElementById('theme-editor-close');
		if (themeEditorCloseButton) {
			themeEditorCloseButton.addEventListener('click', () => setThemeEditorOpen(false));
		}

		const themeEditorBackdrop = document.getElementById('theme-editor-backdrop');
		if (themeEditorBackdrop) {
			themeEditorBackdrop.addEventListener('click', () => setThemeEditorOpen(false));
		}

		document.addEventListener('keydown', (event) => {
			if (event.key === 'Escape') {
				setThemeEditorOpen(false);
			}
		});

		const mainColorInput = document.getElementById('main-color-input');
		if (mainColorInput) {
			mainColorInput.value = getStoredMainColor();
			setMainColorLabel(mainColorInput.value);
			mainColorInput.addEventListener('input', () => {
				setMainColorLabel(mainColorInput.value);
			});
		}

		const applyThemeColorButton = document.getElementById('apply-theme-color');
		if (applyThemeColorButton && mainColorInput) {
			applyThemeColorButton.addEventListener('click', () => {
				applyThemeColor(mainColorInput.value, true);
			});
		}

		const resetThemeColorButton = document.getElementById('reset-theme-color');
		if (resetThemeColorButton) {
			resetThemeColorButton.addEventListener('click', () => {
				localStorage.removeItem(THEME_MAIN_COLOR_KEY);
				applyThemeColor(DEFAULT_MAIN_COLOR, false);
			});
		}

		const tttWinningLines = [
			[0, 1, 2], [3, 4, 5], [6, 7, 8],
			[0, 3, 6], [1, 4, 7], [2, 5, 8],
			[0, 4, 8], [2, 4, 6],
		];
		const TTT_SCORE_KEY = 'tttScore';
		let tttBoard = Array(9).fill('');
		let tttGameOver = false;
		let tttBotPending = false;
		let tttScore = {
			you: 0,
			bot: 0,
			draw: 0,
		};

		function loadTttScore() {
			try {
				const parsed = JSON.parse(localStorage.getItem(TTT_SCORE_KEY) || '{}');
				tttScore = {
					you: Number.isFinite(parsed.you) ? parsed.you : 0,
					bot: Number.isFinite(parsed.bot) ? parsed.bot : 0,
					draw: Number.isFinite(parsed.draw) ? parsed.draw : 0,
				};
			} catch (error) {
				tttScore = { you: 0, bot: 0, draw: 0 };
			}
		}

		function saveTttScore() {
			localStorage.setItem(TTT_SCORE_KEY, JSON.stringify(tttScore));
		}

		function renderTttScore() {
			const you = document.getElementById('ttt-score-you');
			const bot = document.getElementById('ttt-score-bot');
			const draw = document.getElementById('ttt-score-draw');
			if (you) {
				you.textContent = String(tttScore.you);
			}
			if (bot) {
				bot.textContent = String(tttScore.bot);
			}
			if (draw) {
				draw.textContent = String(tttScore.draw);
			}
		}

		function incrementTttScore(outcome) {
			if (outcome === 'X') {
				tttScore.you += 1;
			} else if (outcome === 'O') {
				tttScore.bot += 1;
			} else {
				tttScore.draw += 1;
			}
			saveTttScore();
			renderTttScore();
		}

		function getTttWinner(board) {
			for (const [a, b, c] of tttWinningLines) {
				if (board[a] && board[a] === board[b] && board[b] === board[c]) {
					return board[a];
				}
			}
			return '';
		}

		function renderTttStatus(message) {
			const status = document.getElementById('ttt-status');
			if (status) {
				status.textContent = message;
			}
		}

		function renderTttBoard() {
			document.querySelectorAll('[data-ttt-index]').forEach((cell) => {
				const index = Number.parseInt(cell.dataset.tttIndex || '-1', 10);
				const value = index >= 0 ? tttBoard[index] : '';
				cell.textContent = value;
				cell.disabled = tttGameOver || tttBotPending || !Number.isInteger(index) || value !== '';
			});
		}

		function finalizeTttTurn(nextTurnMessage) {
			const winner = getTttWinner(tttBoard);
			if (winner) {
				tttGameOver = true;
				incrementTttScore(winner);
				renderTttStatus(winner === 'X' ? 'You win!' : 'Bot wins!');
				renderTttBoard();
				return true;
			}
			if (tttBoard.every((cell) => cell !== '')) {
				tttGameOver = true;
				incrementTttScore('draw');
				renderTttStatus('Draw game.');
				renderTttBoard();
				return true;
			}
			renderTttStatus(nextTurnMessage);
			renderTttBoard();
			return false;
		}

		function playTttBotMove() {
			const openIndexes = tttBoard
				.map((value, idx) => (value === '' ? idx : -1))
				.filter((idx) => idx >= 0);
			if (openIndexes.length === 0) {
				tttBotPending = false;
				finalizeTttTurn('Draw game.');
				return;
			}

			const pickedIndex = openIndexes[Math.floor(Math.random() * openIndexes.length)];
			tttBoard[pickedIndex] = 'O';
			tttBotPending = false;
			finalizeTttTurn('Your turn.');
		}

		function onTttPlayerMove(index) {
			if (tttGameOver || tttBotPending || tttBoard[index] !== '') {
				return;
			}
			tttBoard[index] = 'X';
			if (finalizeTttTurn('Bot is thinking...')) {
				return;
			}

			tttBotPending = true;
			renderTttBoard();
			setTimeout(playTttBotMove, 220);
		}

		function resetTttGame() {
			tttBoard = Array(9).fill('');
			tttGameOver = false;
			tttBotPending = false;
			renderTttStatus('Your turn.');
			renderTttBoard();
		}

		document.querySelectorAll('[data-ttt-index]').forEach((cell) => {
			cell.addEventListener('click', () => {
				const index = Number.parseInt(cell.dataset.tttIndex || '-1', 10);
				if (!Number.isInteger(index) || index < 0 || index > 8) {
					return;
				}
				onTttPlayerMove(index);
			});
		});

		const tttResetButton = document.getElementById('ttt-reset');
		if (tttResetButton) {
			tttResetButton.addEventListener('click', resetTttGame);
		}

		const tttResetScoresButton = document.getElementById('ttt-reset-scores');
		if (tttResetScoresButton) {
			tttResetScoresButton.addEventListener('click', () => {
				tttScore = { you: 0, bot: 0, draw: 0 };
				saveTttScore();
				renderTttScore();
			});
		}

		loadTttScore();
		renderTttScore();
		resetTttGame();

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


def _detect_entity(normalized: str, tokens: list[str]) -> str:
		token_set: set[str] = set(tokens)
		for token in list(token_set):
			if token.endswith("ies") and len(token) > 4:
				token_set.add(f"{token[:-3]}y")
			if token.endswith("es") and len(token) > 4:
				token_set.add(token[:-2])
			if token.endswith("s") and len(token) > 3:
				token_set.add(token[:-1])

		paper_cues = {"paper", "article", "publication", "author"}
		method_cues = {"method", "fusion", "approach", "technique", "model"}
		dataset_cues = {"dataset", "data", "sensor", "collection", "source"}

		scores = {
			"paper": sum(1 for cue in paper_cues if cue in token_set),
			"method": sum(1 for cue in method_cues if cue in token_set),
			"dataset": sum(1 for cue in dataset_cues if cue in token_set),
		}

		if scores["paper"] == 0 and scores["method"] == 0 and scores["dataset"] == 0:
			if "field of study" in normalized:
				return "paper"
			return "all"

		best_score = max(scores.values())
		winners = [entity for entity, score in scores.items() if score == best_score and score > 0]
		if len(winners) != 1:
			return "all"
		return winners[0]


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


def _ensure_fts_index(conn: sqlite3.Connection) -> None:
		global FTS_INITIALIZED
		column_defs = ", ".join(FTS_COLUMNS)
		conn.execute(
			f"""
			CREATE VIRTUAL TABLE IF NOT EXISTS {FTS_TABLE}
			USING fts5(
				{column_defs},
				content='unified_records',
				content_rowid='id'
			)
			"""
		)

		base_count = conn.execute("SELECT COUNT(*) FROM unified_records").fetchone()[0]
		index_count = conn.execute(f"SELECT COUNT(*) FROM {FTS_TABLE}").fetchone()[0]

		if (not FTS_INITIALIZED) or index_count != base_count:
			conn.execute(f"INSERT INTO {FTS_TABLE}({FTS_TABLE}) VALUES('rebuild')")
			FTS_INITIALIZED = True


def _build_fts_match_query(entity: str, terms: list[str], quoted_values: list[str]) -> str | None:
		search_terms = quoted_values or terms
		if not search_terms:
			return None

		fts_filler_words = {
			"about", "around", "regarding", "related", "relating", "concerning", "specific",
			"records", "entries", "items",
		}
		entity_terms = {
			"all": {"data", "dataset", "datasets", "paper", "papers", "article", "articles", "method", "methods", "record", "records"},
			"paper": {"paper", "papers", "article", "articles", "record", "records"},
			"method": {"method", "methods", "fusion", "record", "records"},
			"dataset": {"data", "dataset", "datasets", "record", "records"},
		}
		skip_terms = fts_filler_words | entity_terms.get(entity, set())

		fragments: list[str] = []
		for term in search_terms:
			cleaned = re.sub(r"[^a-zA-Z0-9_\-\s]", " ", term).strip()
			if not cleaned:
				continue
			if " " in cleaned:
				fragments.append(f'"{cleaned}"')
			else:
				token = cleaned.lower()
				if token in skip_terms:
					continue

				variants = {token}
				if token.endswith("ies") and len(token) > 4:
					variants.add(f"{token[:-3]}y")
				if token.endswith("es") and len(token) > 4:
					variants.add(token[:-2])
				if token.endswith("s") and len(token) > 3:
					variants.add(token[:-1])

				wildcard_variants = [f"{item}*" for item in sorted(v for v in variants if v)]
				if len(wildcard_variants) == 1:
					fragments.append(wildcard_variants[0])
				else:
					fragments.append("(" + " OR ".join(wildcard_variants) + ")")

		if not fragments:
			return None

		return " AND ".join(fragments)


def _search_sql_for_terms(entity: str, terms: list[str], quoted_values: list[str]) -> tuple[str, list[Any], list[str]]:
		match_query = _build_fts_match_query(entity, terms, quoted_values)
		select_sql = ", ".join(f"u.{column}" for column in RESULT_COLUMNS)

		entity_bonus_sql = "0"
		if entity == "paper":
			entity_bonus_sql = "CASE WHEN u.title IS NOT NULL AND TRIM(u.title) <> '' THEN 1.25 ELSE 0 END"
		elif entity == "method":
			entity_bonus_sql = "CASE WHEN (u.method_name IS NOT NULL OR u.method_key IS NOT NULL) THEN 1.25 ELSE 0 END"
		elif entity == "dataset":
			entity_bonus_sql = """
				CASE WHEN (
					(u.data_name IS NOT NULL AND TRIM(u.data_name) <> '')
					OR (u.data_type IS NOT NULL AND TRIM(u.data_type) <> '')
					OR (u.collection_method IS NOT NULL AND TRIM(u.collection_method) <> '')
					OR (u.dataset_url IS NOT NULL AND TRIM(u.dataset_url) <> '')
				) THEN 1.25 ELSE 0 END
			"""

		match_sql = f"{FTS_TABLE} MATCH ?"
		params: list[Any] = [match_query] if match_query else []
		if not match_query:
			match_sql = "1=1"

		sql = f"""
			SELECT
				{select_sql},
				(-bm25({FTS_TABLE}, 8.0, 2.5, 2.2, 1.2, 1.0, 1.0, 1.0, 1.0, 8.0, 2.0, 1.0, 1.0, 1.0, 8.0, 1.5, 1.2, 1.0, 1.0, 3.0, 2.2, 1.8) + ({entity_bonus_sql})) AS relevance_score
			FROM unified_records AS u
			JOIN {FTS_TABLE} ON {FTS_TABLE}.rowid = u.id
			WHERE {match_sql}
			GROUP BY {select_sql}
			ORDER BY relevance_score DESC, 1 ASC
			LIMIT 100
		"""
		return sql, params, RESULT_COLUMNS + ["relevance_score"]


def _search_sql_like_fallback(entity: str, terms: list[str], quoted_values: list[str]) -> tuple[str, list[Any], list[str]]:
		search_terms = quoted_values or terms
		if not search_terms:
			search_terms = ["*"]

		search_columns = [
			"title", "abstract", "keywords", "field_of_study", "method_name", "data_name", "data_type", "collection_method", "description", "doi",
		]
		select_sql = ", ".join(RESULT_COLUMNS)

		where_parts: list[str] = []
		params: list[Any] = []
		for term in search_terms:
			if term == "*":
				continue
			pattern = f"%{term}%"
			term_checks: list[str] = []
			for col in search_columns:
				term_checks.append(f"LOWER(COALESCE({col}, '')) LIKE LOWER(?)")
				params.append(pattern)
			where_parts.append("(" + " OR ".join(term_checks) + ")")

		where_sql = " OR ".join(where_parts) if where_parts else "1=1"

		entity_bonus_sql = "0"
		if entity == "paper":
			entity_bonus_sql = "CASE WHEN title IS NOT NULL AND TRIM(title) <> '' THEN 1.0 ELSE 0 END"
		elif entity == "method":
			entity_bonus_sql = "CASE WHEN (method_name IS NOT NULL OR method_key IS NOT NULL) THEN 1.0 ELSE 0 END"
		elif entity == "dataset":
			entity_bonus_sql = "CASE WHEN (data_name IS NOT NULL OR data_type IS NOT NULL OR collection_method IS NOT NULL) THEN 1.0 ELSE 0 END"

		sql = f"""
			SELECT
				{select_sql},
				({entity_bonus_sql}) AS relevance_score
			FROM unified_records
			WHERE {where_sql}
			GROUP BY {select_sql}
			ORDER BY relevance_score DESC, 1 ASC
			LIMIT 100
		"""
		return sql, params, RESULT_COLUMNS + ["relevance_score"]


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

		entity = _detect_entity(normalized, tokens)

		sql, params, columns = _search_sql_for_terms(entity, tokens, quoted)
		return sql, params, columns


def _sql_with_params_for_display(sql: str, params: list[Any]) -> str:
		compact_sql = " ".join(sql.split())
		if not params:
			return compact_sql

		display_params = ", ".join(repr(param) for param in params)
		return f"{compact_sql} -- params: [{display_params}]"


def execute_prompt(prompt: str) -> tuple[list[str], list[dict[str, Any]], str, list[str]]:
		with get_conn() as conn:
				_ensure_fts_index(conn)
				sql, params, columns = nl_to_sql(prompt)
				rows = [dict(row) for row in conn.execute(sql, params).fetchall()]

				if not rows and "MATCH ?" in sql:
					normalized = " ".join(prompt.lower().split())
					quoted = _extract_quoted_values(prompt)
					tokens = _tokenize_prompt(prompt)
					fallback_entity = _detect_entity(normalized, tokens)
					fallback_sql, fallback_params, fallback_columns = _search_sql_like_fallback(fallback_entity, tokens, quoted)
					fallback_rows = [dict(row) for row in conn.execute(fallback_sql, fallback_params).fetchall()]
					if fallback_rows:
						sql = fallback_sql
						params = fallback_params
						columns = fallback_columns
						rows = fallback_rows
		visible_columns = _suggest_visible_columns(prompt, columns)
		return columns, rows, _sql_with_params_for_display(sql, params), visible_columns


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
