---
phase: quick
plan: 260819-jal
type: execute
wave: 1
depends_on: []
files_modified: [README.md]
autonomous: true
requirements: []

must_haves:
  truths:
    - "README.md's Executive Summary Polski tab is expanded by default when the page loads"
    - "README.md's Executive Summary English tab is collapsed by default when the page loads"
    - "English tab still appears before the Polski tab in document order"
  artifacts:
    - path: "README.md"
      provides: "Executive Summary language tabs with swapped default-open state"
  key_links: []
---

<objective>
Swap which language tab in README.md's Executive Summary is open by default: currently the English `details` tag (line 3) has the `open` attribute and the Polski `details` tag (line 310) does not. After this change, the Polski tag has `open` and the English tag does not. No other content, order, or markup changes.

Purpose: User-facing default should present Polski expanded, English collapsed, per direct instruction.
Output: Updated README.md with the `open` attribute moved from the English `details` tag to the Polski `details` tag.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
README.md's Executive Summary begins with two top-level collapsible-tab blocks used as language tabs:
- Line 3: `details open` tag / summary "English" — English content, closes at line 308 with the matching closing tag.
- Line 310: `details` tag (no open attribute) / summary "Polski" — Polski content, closes at line 620 with the matching closing tag.

Both blocks contain several nested collapsible sub-tabs for diagrams (opening tags at lines 96, 172, 408, 484, each with their own matching closing tag) — these nested tags are NOT part of this change and must not be touched. Only the two outermost language-tab opening tags (line 3 and line 310) are in scope. Order in the document does not change: English section stays first, Polski section stays second.
</context>

<interfaces>
Not applicable — this is a documentation-only markup edit with no code interfaces.
</interfaces>

<tasks>

<task type="auto">
  <name>Task 1: Swap default-open attribute between English and Polski Executive Summary tabs</name>
  <files>README.md</files>
  <action>
    In README.md, use Edit to make two changes:
    1. Line 3, the English tab's opening tag: remove the `open` attribute so it becomes a plain opening tag (collapsed by default). Target it precisely using its unique surrounding context — this opening tag is immediately followed on the next line by the summary line containing "English".
    2. Line 310, the Polski tab's opening tag: add the `open` attribute so it becomes an open-by-default tag. Target it precisely using its unique surrounding context — this opening tag is immediately followed on the next line by the summary line containing "Polski".
    Do not modify any other opening/closing tag pair in the file, including the nested diagram-toggle tags at lines 96, 172, 408, and 484 and their closers. Do not change document order or any other line content — this is strictly a two-line attribute swap.
  </action>
  <verify>
    <automated>cd /home/konutec/projects/airflow-platform && awk 'NR==3{print} NR==310{print}' README.md | grep -Fxq '<details>' && awk 'NR==3{print} NR==310{print}' README.md | grep -Fxq '<details open>'</automated>
  </verify>
  <done>Line 3 is a bare opening tag with no open attribute (English tab collapsed by default) and line 310 has the open attribute (Polski tab expanded by default). All other collapsible-tab tags in the file (lines 96, 172, 308, 408, 480, 484, 618, 620 and their pairing) are byte-identical to before the edit. Document order (English section first, Polski section second) is unchanged.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

None — documentation-only markup edit to a static file, no code execution, no external input, no runtime behavior.

## STRIDE Threat Register

None applicable — this change has no trust boundary, no user input, no runtime behavior, and no security-relevant surface.
</threat_model>

<verification>
Run: `cd /home/konutec/projects/airflow-platform && git diff README.md`
Confirm the diff touches exactly two lines — line 3 (removes ` open`) and line 310 (adds ` open`) — and nothing else in the file changed.
</verification>

<success_criteria>
- Rendering README.md on GitHub shows the Polski Executive Summary section expanded by default and the English section collapsed by default.
- English tab still appears first in reading order, Polski second — only the default-open state moved.
- `git diff README.md` shows exactly a 2-line change (one attribute removed, one attribute added), no other content touched.
</success_criteria>

<output>
Create `.planning/quick/260819-jal-swap-default-open-language-tab-in-readme/260819-jal-SUMMARY.md` when done
</output>
