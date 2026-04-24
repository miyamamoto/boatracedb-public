---
name: boatrace-program-sheet
description: Use when the user wants a printable BoatRace program sheet, race card PDF, or fixed-layout handout for a target date or venue. Trigger for requests like "番組表を作って", "明日の福岡の番組表をPDFで出して", or "印刷用の出走表を作成して".
---

# BoatRace Program Sheet

This skill creates a fixed-layout, print-friendly BoatRace program sheet PDF.

The user-facing goal is a clean handout, not a backend explanation. Lead with the finished artifact and a short summary of what was generated.

## When To Use

- The user wants `番組表`, `出走表`, `印刷用 PDF`, or a handout-style race summary.
- The output should be stable and printable rather than conversational.
- The user wants one venue or all venues for a given date.

## Default Workflow

1. Make sure the target date already has schedule data and, if needed, prediction data.
2. Generate the PDF with:
   `python3 scripts/boatrace_program_sheet.py --target-date YYYY-MM-DD`
3. To limit output to a venue:
   `python3 scripts/boatrace_program_sheet.py --target-date YYYY-MM-DD --venue-code 22`
4. The output goes to:
   `output/program-sheets/YYYY-MM-DD/`

## Output Rules

- Prefer venue-by-venue PDFs.
- Keep the explanation short: date, venue, file path, and what is inside.
- Do not explain DB tables, JSON, or internal CLI details unless the user explicitly asks.
- If prediction data exists, the PDF should include short racing-language notes such as `本命`, `相手`, `本線`, `押さえ`, `穴`.
- If prediction data does not exist, still generate the program sheet from the schedule data.

## Practical Notes

- `--venue-code` can be repeated when the user wants only selected venues.
- `--races-per-page` controls density. Default is `2`, which is safer for printing.
- If the user asks for tomorrow's program sheet and data is missing, fetch and predict first, then rerun the PDF generation.
