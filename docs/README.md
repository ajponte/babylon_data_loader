# Agent Document Harness

This directory contains the developer and AI agent documentation harness for `babylon_data_loader`. This harness guides AI agents and developers on project structure, agent personas, architectures, and guidelines.

## Harness Index

- **[Agent Personas](agent_personas.md)**: System prompts and instructions for specialized agent roles (Software Engineering, DevOps).
- **[Architecture & Data Flow](architecture.md)**: Technical breakdown of the ingest engine, CSV parser, dual-mode storage layer (Local MongoDB & Cloud MongoDB Atlas), and synthetic data generator.
- **[MongoDB Atlas Datalake Specification](specs/mongodb-atlas-datalake.md)**: Cloud MongoDB Atlas architecture, collection partitioning (`transactions_<datasource>`, `dataSync`), serverless egress security (`0.0.0.0/0`), and AWS Secrets Manager integration.
- **[AWS Lambda Serverless Adapter Spec](specs/lambda/phase1-specification.md)**: Technical specification for event-driven S3 ingestion executed on AWS Lambda (ARM64 Graviton).
- **[Continuous Delivery Pipeline Spec](specs/continuous-delivery-pipeline.md)**: CI/CD architecture, ECR container packaging, automated deployment upon merges to `main`, and release automation.
- **[PII & Security Guidelines](pii_handling.md)**: Deep dive into the PII policies, synthetic data usage, and security parameters of the `babylon` data lake.
- **[Local Development & Testing Guide](development.md)**: Guide on how to set up, test, configure, and troubleshoot the codebase locally.
- **[Agent Planning Docs (`agent-docs/`)](../agent-docs)**: Working notes, prompts, and in-flight planning artifacts produced by AI agents (e.g., the [Wails integration plan](../agent-docs/wails-integration-plan.md) and its [open questions](../agent-docs/wails-open-questions.md)).
