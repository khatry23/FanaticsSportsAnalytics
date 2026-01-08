# Proper Use of Artificial Intelligence

Artificial Intelligence was used deliberately as a productivity and validation aid, not as a replacement for system design judgment. All architectural decisions, tradeoffs, and implementation details were owned, reviewed, and validated by me.

## How AI Was Leveraged

AI was used in the following controlled ways:

### Design exploration and validation
- AI was used to brainstorm architectural options (e.g., ELT vs ETL, direct ingestion vs S3 landing, orchestration boundaries).
- Multiple alternatives were evaluated, and the final design was selected based on correctness, replayability, and alignment with the given tool constraints.

### Drafting and refinement of documentation
- AI assisted in structuring the Technical Design Document to ensure clarity, completeness, and alignment with standard design-review expectations.
- Content was iteratively refined to improve readability and precision, but all technical claims were verified and adjusted based on my own understanding.

### Boilerplate acceleration
AI was used to accelerate generation of repetitive or well-understood artifacts such as:
- Snowflake DDL scaffolding
- Example analytical query templates
- Section outlines for assumptions and tradeoffs

All generated artifacts were reviewed and modified to ensure they matched the problem domain and system requirements.

### Consistency and gap checking
AI was used to cross-check the design against the stated deliverables to ensure no required section (e.g., Snowflake DDL, loading strategy, example queries) was missing or under-specified.
