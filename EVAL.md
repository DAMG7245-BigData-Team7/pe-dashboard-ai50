# RAG vs Structured Evaluation

| company        | method     | factual (0–3) | schema (0–2) | provenance (0–2) | hallucination (0–2) | readability (0–1) | total | notes                                                                                    |
| :------------- | :--------- | :------------ | :----------- | :--------------- | :------------------ | :---------------- | -----: | :--------------------------------------------------------------------------------------- |
| **Anthropic**  | RAG        | 2             | 2            | 1                | 1                   | 1                 |  **7** | Accurate but partially sourced; minor speculation in revenue; no explicit citations.     |
| **Anthropic**  | Structured | 3             | 2            | 2                | 2                   | 1                 | **10** | Fully correct, consistent schema, precise funding, clear segmentation.                   |
| **Captions**   | RAG        | 2             | 2            | 1                | 1                   | 1                 |  **7** | Includes rebrand (Mirage→Captions) but partial provenance and light speculation.         |
| **Captions**   | Structured | 3             | 2            | 2                | 2                   | 1                 | **10** | Accurately captures funding timeline ($125 M total), clean schema, no hallucinations.    |
| **Cohere**     | RAG        | 2             | 2            | 1                | 1                   | 1                 |  **7** | Core facts correct, but general tone (“maintains presence on GitHub”) reduces precision. |
| **Cohere**     | Structured | 3             | 2            | 2                | 2                   | 1                 | **10** | All major rounds listed (A–D, 2024 $1 B total); factual and concise.                     |
| **ElevenLabs** | RAG        | 2             | 2            | 1                | 1                   | 1                 |  **7** | Uses placeholders (“[Not disclosed]”) and lacks concrete sourcing.                       |
| **ElevenLabs** | Structured | 3             | 2            | 2                | 2                   | 1                 | **10** | Verified HQ (London) and $61 M Series A; correct facts and clean sections.               |
| **Notion**     | RAG        | 3             | 2            | 1                | 1                   | 1                 |  **8** | Correct funding ($275 M Series C 2021) and HQ (SF), minor speculative commentary.        |
| **Notion**     | Structured | 3             | 2            | 2                | 2                   | 1                 | **10** | Full factual integrity; Notion 3.0 (2023) accurately captured; strong structure.         |

---

### Reflection summary

This lab marked an important stage in the AI-50 Dashboard project, transitioning from data ingestion to systematic evaluation of model quality. The goal was to compare the RAG (Retrieval-Augmented Generation) and Structured Extraction pipelines using a clear rubric that scored factual accuracy, schema consistency, provenance, hallucination control, and readability across five companies — Anthropic, Captions, Cohere, ElevenLabs, and Notion.

The RAG pipeline produced detailed and context-rich summaries but occasionally lacked source traceability and introduced mild speculation, averaging around 7.2/10. In contrast, the Structured pipeline consistently achieved 10/10, demonstrating superior factual precision, schema alignment, and clean formatting. Its deterministic structure made outputs highly reproducible and ready for downstream analytics.

This evaluation reinforced the value of disciplined scoring frameworks in AI workflows. By quantifying strengths and weaknesses, I learned to identify where retrieval models require grounding and where schema-based systems excel in reliability. The process also emphasized the need for balancing creativity with factual rigor — ensuring that AI-generated outputs remain both informative and trustworthy.

Overall, this lab deepened my understanding of data quality governance and evaluation design. The lessons learned here will directly guide how I implement validation, provenance tracking, and automated scoring mechanisms in future stages of the AI-50 Dashboard and similar RAG-based data pipelines.