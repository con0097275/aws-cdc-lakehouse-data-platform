"""Deterministic analytics for the copilot.

THE SPLIT THIS PACKAGE EXISTS TO ENFORCE: analysis is deterministic and auditable;
narration by a language model is optional and sits on top. "Why did the balance drop?" is a
contribution analysis, not a generation task. "What is it tomorrow?" is a forecast with a
backtested error, not a guess. Building the sentence first produces something that sounds
confident and cannot be checked.
"""
