# Dataiku DSS Plugin - Smart Data Quality

## Overview

The Smart Data Quality Plugin automates the creation and management of data quality rules in Dataiku DSS. It combines statistical profiling with AI-powered natural language processing to help teams establish comprehensive data quality checks across their projects.

## Features

* **Macro - Smart DQ Rules Generator**: Automatically generates 50+ intelligent data quality rules based on statistical dataset profiling. Supports single datasets, multiple selected datasets, or all datasets in a project.
* **Agent Tool - Create Business Rules**: Convert natural language business rules into executable data quality checks through conversational AI. Preview impact before creating rules and iteratively refine conditions.

## Installation

To install the Smart Data Quality Plugin, follow these steps:

1. Download the plugin from the [GitHub releases](https://github.com/ckwonc/smart-dq-plugin/releases).
2. In your Dataiku instance, navigate to Plugins > Add plugin > Upload and select the downloaded plugin package.
3. Follow the on-screen instructions to complete the installation.
4. For the Create Business Rules Tool: Add the tool to an AI agent using the GenAI menu.

## Usage

### Macro Usage - Smart DQ Rules Generator

1. **Select Dataset**: In your Dataiku project, select the dataset you want to generate rules for.
2. **Open Actions Menu**: In the Actions menu on the right side, scroll down and select "Generate Data Quality Rules".
3. **Configure Settings**:
   * Dataset Selection Mode - Choose single, multiple, or all datasets
   * Sample Size - Number of rows to profile (default: 10,000)
   * Strictness Level - Choose lenient (5 sigma), balanced (3 sigma), or strict (2 sigma)
   * Enable Rules - Create rules in enabled or disabled state
   * Skip Existing - Optionally skip datasets that already have DQ rules
3. **Run Macro**: The macro will profile datasets and generate rules based on column statistics.
4. **Review Results**: An HTML report displays creation statistics and links to each dataset's Data Quality tab.

**Rule Types Generated**:
* Categorical: valid values, cardinality checks, most frequent value
* Numeric: non-negative, positive, statistical ranges, ID and year detection
* Nullability: not-null checks for columns with low null rates
* Uniqueness: unique constraints for high-cardinality columns
* Dataset-level: record count and column count stability

### Agent Tool Usage - Create Business Rules

1. **Navigate to Agent Tools**: In your Dataiku project, click the GenAI dropdown in the top left corner of the top bar and select "Agent Tools".
2. **Add Tool**: Click "+ Agent Tool" and select "Create Business Rules" from the list.
3. **Configure Tool** (Optional): Choose a default target dataset (optional). You can override this on a per-request basis.
4. **Describe Rule**: Tell the agent your business rule in plain English (e.g., "customers under age 18 are not allowed").
5. **Analyze Impact**: The agent converts your rule to a pandas condition and shows violation counts. Specify the target dataset if not configured.
6. **Refine or Create**: Request adjustments or approve creation. Rules are tagged with [AI] for easy identification.

**Dataset Specification**:
* You can configure a default dataset when setting up the tool
* Each request can specify a different dataset to override the default
* If no dataset is configured and none is provided in the request, the tool will return an error

**Example Workflow**:
```
User: "Flag any orders where the discount exceeds 50%"
Agent: Analyzes df['discount_pct'] > 50
Agent: Shows "23 rows (0.4%) violate this condition"
User: "Create the rule as a warning"
Agent: Creates rule with business justification stored in metadata
```

## Limitations

**Macro - Smart DQ Rules Generator**:
* String length rules (ColumnLengthInRangeRule) are not supported by the Dataiku API and will be marked for future implementation.
* Date validation rules (ColumnDateNotInFutureRule) are not supported by the Dataiku API and will be marked for future implementation.
* S3 and Spark datasets only support dataset-level rules due to Dataiku API limitations. The rules will still be created, but may error out when rules are computed.
* Wide datasets are limited to 500 columns to prevent memory issues.

**Agent Tool - Create Business Rules**:
* Dataset can be configured at tool setup time or provided per-request (runtime override supported)
* Complex multi-table joins or subqueries are not supported in pandas conditions
* The agent must have access to the dataset schema to validate column names
* Currently supports single dataset only. Future versions will include multi-dataset support with SQL-based analysis and Python-based rule creation

## Support

For issues or feature requests, please contact the plugin maintainer or open an issue on the plugin's GitHub repository.

## Version

Current version: 0.2.1
