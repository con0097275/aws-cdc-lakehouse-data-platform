# ADR-055 — LangGraph yes, LangChain no

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

The brief asks for LangChain. `docs/AI_USE_CASE.md` rejected it for retrieval and named its own revisit trigger: 'agentic tool loops'.

## Options

LangChain everywhere; no framework at all.

## Decision

LangGraph for the agent state machine and multi-tool plans. Tool implementations stay plain functions; authorization stays in `guards.py`; retrieval stays as decided by the AI-P4 gate. Tier 1 must keep working with LangGraph uninstalled.

## Consequences

The trigger the earlier decision named has fired, so this follows that decision rather than reversing it. Its reasoning still governs retrieval, where nothing changed.

## Cost

Per-request only. Generation stays default OFF.

## Security

Authorization must not move into the framework. Adding a node must not become a way to bypass the tool contract.

## Rollback

Uninstall the package and revert; tier 1 continues to work by design, and a test proves it.

## Validation

A test simulates the ImportError and asserts tier 1 still answers.
