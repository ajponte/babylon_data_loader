# Phase 1 Specification: AWS Lambda Serverless Adapter

> [!WARNING]
> **Historical Design Specification — Superseded & Decommissioned**:
> The Phase 1 `/tmp`-staging Lambda architecture described in this document has been permanently decommissioned in **Phase 5C** ([Issue #97](https://github.com/ajponte/babylon_data_loader/issues/97), [PR #107](https://github.com/ajponte/babylon_data_loader/pull/107)).
> 
> The AWS Lambda entrypoint now operates exclusively via pure in-memory streaming through `S3EventHandler` (`cmd/lambda/handler.go`) wired to the hexagonal application pipeline.
> 
> For the authoritative current architecture and runtime specifications, refer to:
> ➡️ **[`docs/specs/hexagonal-application-pipeline.md`](hexagonal-application-pipeline.md)**

This specification document outlines the Phase 1 serverless adapter and packaging architecture for running `babylon_data_loader` on AWS Lambda (ARM64 Graviton).

The complete, authoritative specification is located at:
➡️ **[`docs/specs/lambda/phase1-specification.md`](lambda/phase1-specification.md)**

---

## Quick Navigation:
- **Parent Master Plan**: [`agent-docs/Babylon-Data-Loader-TF-Deploy.md`](../../agent-docs/Babylon-Data-Loader-TF-Deploy.md)
- **Phase 1 Tracking Plan**: [`agent-docs/DATA-LOADER-TF-PHASE1.md`](../../agent-docs/DATA-LOADER-TF-PHASE1.md)
- **Technical Audit & Status**: [`agent-docs/DATA-LOADER-LAMBDA-STATUS.md`](../../agent-docs/DATA-LOADER-LAMBDA-STATUS.md)
- **Wrap-Up Plan**: [`agent-docs/DATA-LOADER-LAMBDA-PHASE1-PLAN.md`](../../agent-docs/DATA-LOADER-LAMBDA-PHASE1-PLAN.md)
- **Primary Detailed Specification**: [`docs/specs/lambda/phase1-specification.md`](lambda/phase1-specification.md)
