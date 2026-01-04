# Case Studies: Real-World Airflow at Scale

This directory contains case studies examining how leading technology companies use Apache Airflow in production. Each case study analyzes architectural patterns, lessons learned, and code examples you can apply to your own workflows.

## Available Case Studies

| Case Study                                            | Company  | Focus Area   | Key Patterns                                    |
| ----------------------------------------------------- | -------- | ------------ | ----------------------------------------------- |
| [Spotify Recommendations](spotify-recommendations.md) | Spotify  | ML Pipelines | Dynamic tasks, ML model training, A/B testing   |
| [Stripe Fraud Detection](stripe-fraud-detection.md)   | Stripe   | Real-Time ML | Low-latency pipelines, feature engineering      |
| [Airbnb Experimentation](airbnb-experimentation.md)   | Airbnb   | A/B Testing  | Experiment orchestration, metrics pipelines     |
| [Modern RAG Architecture](modern-rag-architecture.md) | Industry | AI/ML        | RAG pipelines, vector stores, LLM orchestration |

## How to Use These Case Studies

### For Learning

1. **Read the context** - Understand the company's scale and challenges
2. **Study the architecture** - See how patterns fit together
3. **Apply to exercises** - Connect patterns to curriculum exercises
4. **Reference during projects** - Use as templates for real work

### For Production Reference

Each case study includes:

- **Architecture diagrams** - Visual representation of data flows
- **Pattern breakdown** - Which Airflow features solve which problems
- **Code examples** - Simplified implementations you can adapt
- **Lessons learned** - What worked and what didn't
- **Related exercises** - Curriculum exercises that teach these patterns

## Pattern Cross-Reference

| Pattern                 | Case Studies        | Curriculum Modules |
| ----------------------- | ------------------- | ------------------ |
| Dynamic Task Mapping    | Spotify, Airbnb     | Module 06          |
| Deferrable Sensors      | Stripe, Modern RAG  | Module 11          |
| KubernetesExecutor      | All                 | Module 08          |
| Asset-Driven Scheduling | Spotify, Modern RAG | Module 05          |
| Production Monitoring   | Stripe, Spotify     | Module 09          |
| LLM Integration         | Modern RAG          | Module 15          |

## Contributing

These case studies are synthesized from:

- Public engineering blogs and conference talks
- Open-source Airflow provider packages
- Community discussions and best practices
- Industry-standard patterns and architectures

The examples are simplified for educational purposes while maintaining authentic patterns.

---

[← Back to Documentation](../README.md) | [Module 15: AI/ML →](../../modules/15-ai-ml-orchestration/)
