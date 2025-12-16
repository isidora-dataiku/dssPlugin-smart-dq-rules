"""
Business Rule Creator Agent Tool
Creates custom Python Data Quality rules from structured rule definitions
"""

from dataiku.llm.agent_tools import BaseAgentTool
import logging
import dataiku
import json
import textwrap
from datetime import datetime


class BusinessRuleCreator(BaseAgentTool):
    """Creates custom DQ rules from structured rule definitions"""
    
    def set_config(self, config, plugin_config):
        """Initialize the tool with configuration"""
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.plugin_config = plugin_config
        self.dataset_name = config.get("dataset")
        
    def get_descriptor(self, tool):
        """Define the tool's interface for the LLM"""
        return {
            "description": """Create or analyze a custom data quality rule. You must provide the Python condition - this tool does not parse natural language.

Use this tool to:
- Analyze impact: See how many rows violate a condition
- Create rule: Actually create the data quality rule
- Refine: Update a condition and re-analyze

You (the agent) are responsible for converting business logic into Python pandas conditions.""",
            
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["analyze", "create"],
                        "description": "What to do: 'analyze' to preview impact, 'create' to make the rule"
                    },
                    "python_condition": {
                        "type": "string",
                        "description": "Python pandas condition (e.g., \"df['age'] < 20\" or \"df['price'] > (df['income'] * 3)\")"
                    },
                    "columns_used": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of column names used in the condition"
                    },
                    "rule_name": {
                        "type": "string",
                        "description": "Short identifier for the rule (e.g., 'age_restriction_promos')"
                    },
                    "business_rule": {
                        "type": "string",
                        "description": "Original business rule description in plain English"
                    },
                    "justification": {
                        "type": "string",
                        "description": "Why this rule exists (regulatory, business policy, etc.)"
                    },
                    "severity": {
                        "type": "string",
                        "enum": ["ERROR", "WARNING"],
                        "description": "Severity level. Defaults to WARNING."
                    }
                },
                "required": ["action", "python_condition", "columns_used", "rule_name", "business_rule"]
            }
        }
    
    def invoke(self, input, trace):
        """Execute the tool"""
        self.logger.info(f"Business Rule Creator invoked: {input}")
        
        try:
            args = input.get("input", {})
            
            # SECURITY: Strict input validation
            action = args.get("action")
            if action not in ["analyze", "create"]:
                return {
                    "output": json.dumps({
                        "success": False,
                        "error": "Invalid action. Must be 'analyze' or 'create'"
                    })
                }
            
            python_condition = args.get("python_condition", "").strip()
            if not python_condition:
                return {
                    "output": json.dumps({
                        "success": False,
                        "error": "python_condition is required"
                    })
                }
            
            # SECURITY: Validate columns_used is a list
            columns_used = args.get("columns_used", [])
            if not isinstance(columns_used, list) or len(columns_used) == 0:
                return {
                    "output": json.dumps({
                        "success": False,
                        "error": "columns_used must be a non-empty list of column names"
                    })
                }
            
            rule_name = args.get("rule_name", "").strip()
            business_rule = args.get("business_rule", "").strip()
            justification = args.get("justification", "Business requirement")
            severity = args.get("severity", "WARNING")
            
            if not rule_name or not business_rule:
                return {
                    "output": json.dumps({
                        "success": False,
                        "error": "rule_name and business_rule are required"
                    })
                }
            
            # Get dataset
            dataset = dataiku.Dataset(self.dataset_name)
            
            # Get schema
            schema = dataset.read_schema()
            available_columns = {col["name"]: col["type"] for col in schema}
            
            # Validate columns exist
            validation = self._validate_columns(columns_used, available_columns)
            if not validation["valid"]:
                return {
                    "output": json.dumps({
                        "success": False,
                        "error": validation["error"],
                        "available_columns": list(available_columns.keys())
                    })
                }
            
            # Analyze impact
            if action == "analyze":
                impact = self._analyze_impact(dataset, python_condition, columns_used)
                
                if "error" in impact:
                    return {
                        "output": json.dumps({
                            "success": False,
                            "error": f"Failed to evaluate condition: {impact['error']}"
                        })
                    }
                
                human_readable = self._convert_to_human_readable(python_condition, columns_used)
                
                return {
                    "output": json.dumps({
                        "success": True,
                        "action": "analyze",
                        "impact": {
                            "total_rows": impact["total_rows"],
                            "violating_rows": impact["violating_rows"],
                            "passing_rows": impact["passing_rows"],
                            "violation_percentage": round(impact["violation_percentage"], 2),
                            "passing_percentage": round(impact["passing_percentage"], 2)
                        },
                        "columns_used": columns_used,
                        "python_condition": python_condition,
                        "human_readable_condition": human_readable,
                        "guidance": self._get_impact_guidance(impact["violation_percentage"])
                    })
                }
            
            # Create the rule
            elif action == "create":
                result = self._create_dq_rule(
                    dataset=self.dataset_name,
                    rule_name=rule_name,
                    business_rule=business_rule,
                    justification=justification,
                    python_condition=python_condition,
                    columns_used=columns_used,
                    severity=severity
                )
                
                if result["success"]:
                    return {
                        "output": json.dumps({
                            "success": True,
                            "action": "create",
                            "rule_name": result["rule_name"],
                            "rule_url": result["rule_url"],
                            "message": f"Rule '{result['rule_name']}' created successfully"
                        })
                    }
                else:
                    return {
                        "output": json.dumps({
                            "success": False,
                            "error": result["error"]
                        })
                    }
            
        except Exception as e:
            self.logger.error(f"Error in BusinessRuleCreator: {e}", exc_info=True)
            return {
                "output": json.dumps({
                    "success": False,
                    "error": str(e)
                })
            }
    
    def _validate_columns(self, columns_used, available_columns):
        """Check if all columns exist"""
        missing = [col for col in columns_used if col not in available_columns]
        
        if missing:
            return {
                "valid": False,
                "error": f"Columns not found in dataset: {', '.join(missing)}"
            }
        
        return {"valid": True}
    
    def _analyze_impact(self, dataset, python_condition, columns_used):
        """Preview how many rows would be affected"""
        try:
            # SECURITY FIX: Only load necessary columns (memory optimization)
            df = dataset.get_dataframe(limit=10000, columns=columns_used)
            total_rows = len(df)
            
            if total_rows == 0:
                return {
                    "total_rows": 0,
                    "violating_rows": 0,
                    "passing_rows": 0,
                    "violation_percentage": 0,
                    "passing_percentage": 0,
                    "error": "Dataset is empty"
                }
            
            # SECURITY FIX: Use restricted eval with limited scope
            safe_locals = {"df": df}
            safe_globals = {
                "__builtins__": None  # Block access to open, import, exec, etc.
            }
            
            # Evaluate the condition
            violating_mask = eval(python_condition, safe_globals, safe_locals)
            
            # Verify result is a boolean series
            if not hasattr(violating_mask, 'sum'):
                raise ValueError("Condition did not return a pandas Series/Boolean mask")
            
            violating_rows = violating_mask.sum()
            passing_rows = total_rows - violating_rows
            
            return {
                "total_rows": total_rows,
                "violating_rows": int(violating_rows),
                "passing_rows": int(passing_rows),
                "violation_percentage": (violating_rows / total_rows * 100) if total_rows > 0 else 0,
                "passing_percentage": (passing_rows / total_rows * 100) if total_rows > 0 else 0
            }
        except Exception as e:
            self.logger.error(f"Impact analysis failed: {e}", exc_info=True)
            return {
                "total_rows": 0,
                "violating_rows": 0,
                "passing_rows": 0,
                "violation_percentage": 0,
                "passing_percentage": 0,
                "error": str(e)
            }
    
    def _get_impact_guidance(self, violation_percentage):
        """Provide guidance based on violation rate"""
        if violation_percentage > 50:
            return "Over 50% violations - rule might be too strict or data has quality issues"
        elif violation_percentage > 20:
            return "20-50% violations - verify this is expected"
        elif violation_percentage < 1:
            return "Low violation rate (<1%)"
        else:
            return "Reasonable violation rate"
    
    def _convert_to_human_readable(self, python_condition, columns_used):
        """Convert Python condition to human-readable format"""
        # Remove df[] notation and make it readable
        readable = python_condition
        
        # Replace df['column'] with Column Name (title case)
        for col in columns_used:
            # Convert snake_case to Title Case
            title_col = col.replace('_', ' ').title()
            readable = readable.replace(f"df['{col}']", title_col)
        
        # Replace operators
        readable = readable.replace('&', 'AND')
        readable = readable.replace('|', 'OR')
        readable = readable.replace('~', 'NOT')
        readable = readable.replace('.isin(', ' IN ')
        
        # Clean up extra parentheses and whitespace
        readable = readable.replace('(', '').replace(')', '')
        readable = ' '.join(readable.split())
        
        return readable
    
    def _create_dq_rule(self, dataset, rule_name, business_rule, justification, python_condition, columns_used, severity):
        """Create the actual Data Quality rule"""
        try:
            client = dataiku.api_client()
            project = client.get_default_project()
            dataset_handle = project.get_dataset(dataset)
            dq_ruleset = dataset_handle.get_data_quality_rules()

            # Add [BR] tag to rule name
            rule_name_with_tag = f"{rule_name} [BR]"

            # SECURITY FIX: Sanitize condition - remove newlines, limit to single line
            clean_condition = python_condition.replace("\n", " ").replace("\r", " ").strip()

            # FIX 1: Format columns list as a python string list for the injected code
            # We need the string representation of the list: "['col1', 'col2']"
            columns_code_str = str(columns_used)

            # SECURITY FIX: Use textwrap.dedent for proper indentation
            python_code = textwrap.dedent(f'''\
                def process(last_values, dataset, partition_id):
                    """
                    [AI-GENERATED BUSINESS RULE]
                    This metadata was automatically generated based on user input.
                    
                    Business Rule: {business_rule}
                    Justification: {justification}
                    Created: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                    Columns: {", ".join(columns_used)}
                    Condition: {clean_condition}
                    """
                    import pandas as pd

                    try:
                        # FIX 2: Only load specific columns to prevent OOM in production
                        # We inject the list of columns we validated earlier
                        df = dataset.get_dataframe(columns={columns_code_str})

                        # Apply business rule with restricted scope
                        safe_locals = {{"df": df}}
                        safe_globals = {{"__builtins__": None}}

                        # FIX 3: Use triple quotes to handle mixed quoting in the condition
                        # Example: df['col'] == "value" won't break the string
                        condition_str = """{clean_condition}"""

                        violating_mask = eval(condition_str, safe_globals, safe_locals)
                        violation_count = violating_mask.sum()
                        total_rows = len(df)

                        # Determine status
                        if violation_count == 0:
                            status = 'OK'
                            message = f'All {{total_rows}} rows comply with business rule: {business_rule}'
                        else:
                            violation_pct = (violation_count / total_rows * 100) if total_rows > 0 else 0
                            status = '{"ERROR" if severity == "ERROR" else "WARNING"}'
                            message = f'{{violation_count}} rows ({{violation_pct:.1f}}%) violate rule: {business_rule}'

                        return status, message

                    except Exception as e:
                        return 'ERROR', f'Rule execution failed: {{str(e)}}'
            ''')

            # Create the rule
            rule_config = {
                "type": "python",
                "displayName": rule_name_with_tag,
                "code": python_code,
                "enabled": True,
                "autoRun": True,
                "computeOnBuildMode": "PARTITION",
                "envSelection": {
                    "envMode": "USE_BUILTIN_MODE",
                    "envName": ""
                },
                "meta": {
                    "label": business_rule,
                    "name": rule_name_with_tag
                }
            }

            # Create the rule
            rule = dq_ruleset.create_rule(rule_config)

            # Build URL
            project_key = project.project_key
            rule_url = f"/projects/{project_key}/datasets/{dataset}/data-quality"

            return {
                "success": True,
                "rule_name": rule_name_with_tag,
                "rule_url": rule_url
            }

        except Exception as e:
            self.logger.error(f"Failed to create DQ rule: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
