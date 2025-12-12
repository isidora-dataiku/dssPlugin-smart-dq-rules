"""
API Client Module
Handles creation of data quality rules via Dataiku API
"""

import time
from typing import Dict, List
import textwrap


class DataQualityAPIClient:
    """Creates data quality rules via Dataiku API"""
    
    def __init__(self, project, dataset_name: str, skip_existing: bool = True):
        """
        Initialize the API client
        
        Args:
            project: DSSProject object
            dataset_name: Name of the dataset
            skip_existing: Whether to skip if rules already exist
        """
        self.project = project
        self.dataset_name = dataset_name
        self.skip_existing = skip_existing
        
        # Get dataset handle
        self.dataset_handle = project.get_dataset(dataset_name)
        self.dq_ruleset = self.dataset_handle.get_data_quality_rules()
        
        # Get existing rules
        self.existing_rule_names = []
        if skip_existing:
            try:
                existing_rules = self.dq_ruleset.list_rules(as_type='objects')
                self.existing_rule_names = [rule.name for rule in existing_rules]
                print(f"✅ Found {len(self.existing_rule_names)} existing rules")
            except Exception as e:
                print(f"⚠️ Could not fetch existing rules: {e}")
    
    def create_rules(self, generated_rules: List[Dict]) -> Dict:
        """
        Create all generated rules via API
        
        Args:
            generated_rules: List of rule configurations
            
        Returns:
            Dictionary with creation results
        """
        print("\n" + "="*80)
        print("CREATING RULES IN DATAIKU")
        print("="*80)
        
        results = {
            'created': [],
            'skipped': [],
            'failed': [],
            'unsupported': []
        }
        
        for i, rule_config in enumerate(generated_rules, 1):
            rule_name = rule_config['name']
            
            print(f"\n[{i}/{len(generated_rules)}] Processing: {rule_name}")
            
            # Check if rule already exists
            if rule_name in self.existing_rule_names:
                print(f"  ⏭️  SKIPPED - Rule already exists")
                results['skipped'].append({
                    'name': rule_name,
                    'reason': 'already_exists'
                })
                continue
            
            # Convert to API format
            api_config = self._convert_rule_to_api_format(rule_config)
            
            if api_config is None:
                results['unsupported'].append({
                    'name': rule_name,
                    'type': rule_config['type']
                })
                continue
            
            # Try to create the rule
            try:
                new_rule = self.dq_ruleset.create_rule(api_config)
                rule_type_display = f"{api_config['type']} (Custom Python)" if api_config['type'] == 'python' else api_config['type']
                print(f"  ✅ CREATED - ID: {new_rule.id} - Type: {rule_type_display}")
                results['created'].append({
                    'name': rule_name,
                    'id': new_rule.id,
                    'type': api_config['type']
                })
                
                # Small delay to avoid API rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                error_msg = str(e)
                print(f"  ❌ FAILED - {error_msg[:100]}")
                results['failed'].append({
                    'name': rule_name,
                    'type': rule_config['type'],
                    'reason': error_msg[:200]
                })
        
        print("\n" + "="*80)
        print(f"✅ Successfully created: {len(results['created'])} rules")
        print(f"⏭️  Skipped: {len(results['skipped'])} rules")
        print(f"⚠️  Unsupported: {len(results['unsupported'])} rules")
        print(f"❌ Failed: {len(results['failed'])} rules")
        
        return results
    
    def _convert_rule_to_api_format(self, rule_config: Dict) -> Dict:
        """
        Convert our rule configuration to Dataiku API format
        
        Returns:
            dict in Dataiku API format, or None if conversion fails
        """
        try:
            rule_type = rule_config['type']
            rule_name = rule_config['name']
            params = rule_config['params']
            severity = rule_config['severity']
            enabled = rule_config.get('enabled', True)
            
            # Base config for all rules
            api_config = {
                'displayName': rule_name,
                'enabled': enabled,
                'autoRun': True,
                'computeOnBuildMode': 'PARTITION'
            }
            
            # Get column name (most rules have this)
            col = params.get('column')
            
            # =====================================================================
            # CATEGORICAL RULES
            # =====================================================================
            
            if rule_type == 'ColumnValuesInSetRule':
                api_config['type'] = 'ValuesInSetRule'
                api_config['columns'] = [col]
                api_config['valueSet'] = params['values']
            
            elif rule_type == 'ColumnDistinctCountInRangeRule':
                # Create as custom Python rule since no built-in exists
                soft_min = params.get('softMinimum', 1)
                soft_max = params.get('softMaximum', 100)

                # SECURITY: Escape single quotes in column names
                safe_col = col.replace("'", "\\'")

                python_code = textwrap.dedent(f"""\
                    def process(last_values, dataset, partition_id):
                        '''
                        Check if distinct value count for {safe_col} is within expected range
                        Expected range: {soft_min} - {soft_max} distinct values

                        :param dict(str, dataiku.metrics.MetricDataPoint) last_values: last values of the metrics
                        :param dataiku.Dataset dataset: dataset on which the rule is computed
                        :param str partition_id: The id of the partition where the rule is computed
                        :return: (state, message) with state one of 'Error', 'Warning', or 'OK'
                        '''
                        import pandas as pd

                        try:
                            # Read the dataset
                            df = dataset.get_dataframe()

                            # Count distinct values in the column
                            distinct_count = df['{safe_col}'].nunique()

                            # Determine status based on thresholds
                            if distinct_count < {soft_min} or distinct_count > {soft_max}:
                                status = 'Warning'
                                message = f'Distinct count {{distinct_count}} is outside expected range ({soft_min}-{soft_max})'
                            else:
                                status = 'OK'
                                message = f'Distinct count {{distinct_count}} is within expected range ({soft_min}-{soft_max})'

                            return status, message

                        except Exception as e:
                            return 'ERROR', f'Rule execution failed: {{str(e)}}'
                """)

                api_config['type'] = 'python'
                api_config['code'] = python_code
                api_config['envSelection'] = {
                    'envMode': 'USE_BUILTIN_MODE',
                    'envName': ''
                }
                api_config['meta'] = {
                    'label': '',
                    'name': ''
                }
            
            elif rule_type == 'ColumnMostFrequentValueInSetRule':
                api_config['type'] = 'ModeValueInSetRule'
                api_config['columns'] = [col]
                api_config['valueSet'] = params['values']
            
            # =====================================================================
            # NUMERIC RULES
            # =====================================================================
            
            elif rule_type == 'ColumnMinInRangeRule':
                api_config['type'] = 'ColumnMinInRangeRule'
                api_config['columns'] = [col]
                if params.get('minimumEnabled'):
                    api_config['minimum'] = float(params['minimum'])
                    api_config['minimumEnabled'] = True
                if params.get('softMinimumEnabled'):
                    api_config['softMinimum'] = float(params.get('softMinimum', 0))
                    api_config['softMinimumEnabled'] = True
            
            elif rule_type == 'ColumnMaxInRangeRule':
                api_config['type'] = 'ColumnMaxInRangeRule'
                api_config['columns'] = [col]
                if params.get('maximumEnabled'):
                    api_config['maximum'] = float(params['maximum'])
                    api_config['maximumEnabled'] = True
                if params.get('softMaximumEnabled'):
                    api_config['softMaximum'] = float(params.get('softMaximum', 0))
                    api_config['softMaximumEnabled'] = True
            
            elif rule_type == 'ColumnValueInRangeRule':
                # Use ColumnAvgInRangeRule for statistical ranges
                api_config['type'] = 'ColumnAvgInRangeRule'
                api_config['columns'] = [col]
                if params.get('softMinimumEnabled'):
                    api_config['softMinimum'] = float(params['softMinimum'])
                    api_config['softMinimumEnabled'] = True
                if params.get('softMaximumEnabled'):
                    api_config['softMaximum'] = float(params['softMaximum'])
                    api_config['softMaximumEnabled'] = True
            
            # =====================================================================
            # STRING RULES
            # =====================================================================
            
            elif rule_type == 'ColumnMatchesPatternRule':
                print(f"  ⚠️ Pattern matching not yet supported, skipping")
                return None
            
            elif rule_type == 'ColumnLengthInRangeRule':
                print(f"  ⚠️ String length rules not yet supported, skipping")
                return None
            
            # =====================================================================
            # DATE RULES
            # =====================================================================
            
            elif rule_type == 'ColumnDateNotInFutureRule':
                print(f"  ⚠️ Date rules not yet supported, skipping")
                return None
            
            # =====================================================================
            # NULLABILITY RULES
            # =====================================================================
            
            elif rule_type == 'ColumnValuesNotEmptyRule':
                api_config['type'] = 'ColumnNotEmptyRule'
                api_config['columns'] = [col]
                
                threshold_type = params.get('thresholdType', 'all')
                if threshold_type == 'all':
                    api_config['thresholdType'] = 'ENTIRE_COLUMN_NOT_EMPTY'
                elif threshold_type == 'proportion':
                    api_config['thresholdType'] = 'NUMBER_NOT_EMPTY'
                    max_prop = params.get('maxProportion', 0.05)
                    api_config['softMaximum'] = max_prop * 100
                    api_config['softMaximumEnabled'] = True
            
            # =====================================================================
            # UNIQUENESS RULES
            # =====================================================================
            
            elif rule_type == 'ColumnValuesUniqueRule':
                api_config['type'] = 'ColumnUniqueValuesRule'
                api_config['columns'] = [col]
                api_config['thresholdType'] = 'ENTIRE_COLUMN'
            
            # =====================================================================
            # DATASET-LEVEL RULES
            # =====================================================================
            
            elif rule_type == 'RecordCountInRangeRule':
                api_config['type'] = 'RecordCountInRangeRule'
                # No columns parameter for dataset-level rules
                if params.get('minimumEnabled'):
                    api_config['minimum'] = float(params['minimum'])
                    api_config['minimumEnabled'] = True
                if params.get('softMinimumEnabled'):
                    api_config['softMinimum'] = float(params['softMinimum'])
                    api_config['softMinimumEnabled'] = True
                if params.get('maximumEnabled'):
                    api_config['maximum'] = float(params['maximum'])
                    api_config['maximumEnabled'] = True
                if params.get('softMaximumEnabled'):
                    api_config['softMaximum'] = float(params['softMaximum'])
                    api_config['softMaximumEnabled'] = True
            
            elif rule_type == 'ColumnCountInRangeRule':
                api_config['type'] = 'ColumnCountInRangeRule'
                if params.get('minimumEnabled'):
                    api_config['minimum'] = int(params['minimum'])
                    api_config['minimumEnabled'] = True
                if params.get('maximumEnabled'):
                    api_config['maximum'] = int(params['maximum'])
                    api_config['maximumEnabled'] = True
            
            # =====================================================================
            # UNKNOWN RULE TYPE
            # =====================================================================
            
            else:
                print(f"  ⚠️ Unknown rule type: {rule_type}")
                return None
            
            return api_config
        
        except Exception as e:
            print(f"  ❌ Error converting rule: {e}")
            import traceback
            traceback.print_exc()
            return None
