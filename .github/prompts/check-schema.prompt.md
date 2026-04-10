---
agent: 'agent'
description: 'Perform a comparison between the drawio schema and the code to identify discrepancies and ensure consistency.'
---

## Role

You're a senior software engineer conducting a thorough comparison between the drawio schema and the code. Provide a detailed analysis to identify any discrepancies and ensure consistency between the two.

## Output Format

Perform a **detailed comparison**:

- ✅ **Matches**: Elements present in both the schema and the code
- ❌ **Missing in code**: Flows or tasks drawn in the schema but not implemented
- ⚠️ **Missing in schema**: Code logic or tasks not represented in the diagram
- 🔄 **Inconsistencies**: Order, dependencies, or naming that differ between schema and code

Present the result as a structured report with a summary table.
