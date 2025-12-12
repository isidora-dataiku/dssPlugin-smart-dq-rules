"""
Rule Generator Module
Generates intelligent data quality rules based on dataset profile
"""

from typing import Dict, List


class RuleGenerator:
    """Generates data quality rules based on dataset profile"""
    
    def __init__(self, profile: Dict, strictness: str = "balanced", enable_rules: bool = True):
        """
        Initialize the rule generator
        
        Args:
            profile: Dataset profile from DatasetProfiler
            strictness: 'lenient', 'balanced', or 'strict'
            enable_rules: Whether rules should be enabled after creation
        """
        self.profile = profile
        self.strictness = strictness
        self.enable_rules = enable_rules
        self.rules = []
        
        # Sigma multipliers for different strictness levels
        self.sigma_multipliers = {
            'lenient': 5.0,
            'balanced': 3.0,
            'strict': 2.0
        }
    
    def generate_all_rules(self) -> List[Dict]:
        """
        Generate all rules for the dataset
        
        Returns:
            List of rule configurations
        """
        print("\n" + "="*80)
        print("GENERATING DATA QUALITY RULES")
        print("="*80)
        
        self.rules = []
        
        # Generate rules by category
        self._generate_categorical_rules()
        self._generate_numeric_rules()
        self._generate_string_rules()
        self._generate_date_rules()
        self._generate_nullability_rules()
        self._generate_uniqueness_rules()
        self._generate_dataset_level_rules()
        
        print(f"\n📊 Total rules generated: {len(self.rules)}")
        return self.rules
    
    def _create_rule_config(self, rule_type: str, name: str, params: Dict, severity: str = 'WARNING') -> Dict:
        """Helper to create a rule configuration"""
        return {
            'type': rule_type,
            'name': name,
            'params': params,
            'severity': severity,
            'enabled': self.enable_rules
        }
    
    def _generate_categorical_rules(self):
        """Generate rules for categorical columns"""
        print("\n" + "="*80)
        print("GENERATING CATEGORICAL COLUMN RULES")
        print("="*80)
        
        for col_name in self.profile['categorization']['categorical']:
            col_profile = next(c for c in self.profile['columns'] if c['name'] == col_name)
            
            print(f"\n🏷️  {col_name}")
            
            # Rule 1: Valid values (if cardinality is reasonable)
            if col_profile.get('cardinality', 0) <= 20:
                values = list(col_profile.get('top_values', {}).keys())
                rule = self._create_rule_config(
                    rule_type='ColumnValuesInSetRule',
                    name=f"{col_name}_valid_values",
                    params={'column': col_name, 'values': values},
                    severity='ERROR'
                )
                self.rules.append(rule)
                print(f"  ✅ Created: Valid values rule ({len(values)} values)")
            
            # Rule 2: Cardinality check
            cardinality = col_profile.get('cardinality', 0)
            if cardinality <= 10:
                tolerance = 5
            elif cardinality <= 50:
                tolerance = 10
            else:
                tolerance = 20
            
            soft_min = max(1, cardinality - tolerance)
            soft_max = cardinality + tolerance
            
            rule = self._create_rule_config(
                rule_type='ColumnDistinctCountInRangeRule',
                name=f"{col_name}_cardinality",
                params={
                    'column': col_name,
                    'softMinimum': soft_min,
                    'softMaximum': soft_max,
                    'softMinimumEnabled': True,
                    'softMaximumEnabled': True
                }
            )
            self.rules.append(rule)
            print(f"  ✅ Created: Cardinality rule ({soft_min}-{soft_max})")
            
            # Rule 3: Most frequent value check
            if col_profile.get('most_frequent_value'):
                rule = self._create_rule_config(
                    rule_type='ColumnMostFrequentValueInSetRule',
                    name=f"{col_name}_top_value",
                    params={'column': col_name, 'values': [col_profile['most_frequent_value']]}
                )
                self.rules.append(rule)
                print(f"  ✅ Created: Top value rule")
    
    def _generate_numeric_rules(self):
        """Generate rules for numeric columns"""
        print("\n" + "="*80)
        print("GENERATING NUMERIC COLUMN RULES")
        print("="*80)
        
        for col_name in self.profile['categorization']['numeric']:
            col_profile = next(c for c in self.profile['columns'] if c['name'] == col_name)
            
            print(f"\n🔢 {col_name}")
            
            stats = col_profile.get('stats', {})
            if not stats:
                print(f"  ⚠️ No stats available, skipping")
                continue
            
            mean = float(stats['mean'])
            std = float(stats['std'])
            min_val = float(stats['min'])
            max_val = float(stats['max'])
            
            col_lower = col_name.lower()
            
            # ID Detection (case-insensitive)
            is_id_column = (
                (col_lower.endswith('_id') or col_lower.endswith('_key') or col_lower.endswith('_code') or
                 col_lower.startswith('id_') or col_lower.startswith('key_') or col_lower.startswith('code_') or
                 col_lower in ['id', 'key', 'code']) and
                col_profile.get('unique_ratio', 0) > 0.95
            )
            
            # Year Detection
            is_year_column = 'year' in col_lower and min_val >= 1900 and max_val <= 2100
            
            # Rule 1: Non-negative
            if col_profile.get('all_non_negative', False):
                rule = self._create_rule_config(
                    rule_type='ColumnMinInRangeRule',
                    name=f"{col_name}_non_negative",
                    params={'column': col_name, 'minimum': 0, 'minimumEnabled': True},
                    severity='ERROR'
                )
                self.rules.append(rule)
                print(f"  ✅ Created: Non-negative rule (min >= 0)")
            
            # Rule 2: Positive (for price/amount columns)
            if any(keyword in col_lower for keyword in ['amount', 'price', 'cost', 'fee', 'charge', 'payment']):
                if min_val > 0:
                    rule = self._create_rule_config(
                        rule_type='ColumnMinInRangeRule',
                        name=f"{col_name}_positive",
                        params={'column': col_name, 'minimum': 0.0, 'minimumEnabled': True},
                        severity='ERROR'
                    )
                    self.rules.append(rule)
                    print(f"  ✅ Created: Positive value rule (min > 0)")
            
            # Rule 3: Range rules
            if is_id_column:
                print(f"  ⏭️  Skipped: Range rule (ID column with high uniqueness)")
            elif is_year_column:
                # Year min rule
                rule = self._create_rule_config(
                    rule_type='ColumnMinInRangeRule',
                    name=f"{col_name}_reasonable_min",
                    params={'column': col_name, 'softMinimum': int(min_val) - 5, 'softMinimumEnabled': True}
                )
                self.rules.append(rule)
                
                # Year max rule
                rule = self._create_rule_config(
                    rule_type='ColumnMaxInRangeRule',
                    name=f"{col_name}_reasonable_max",
                    params={'column': col_name, 'softMaximum': int(max_val) + 5, 'softMaximumEnabled': True}
                )
                self.rules.append(rule)
                print(f"  ✅ Created: Year range rules ({int(min_val)-5} to {int(max_val)+5})")
            else:
                # Statistical range
                sigma_mult = self.sigma_multipliers[self.strictness]
                soft_min = mean - sigma_mult * std
                soft_max = mean + sigma_mult * std
                
                if col_profile.get('all_non_negative', False) and soft_min < 0:
                    soft_min = 0.0
                
                rule = self._create_rule_config(
                    rule_type='ColumnValueInRangeRule',
                    name=f"{col_name}_range",
                    params={
                        'column': col_name,
                        'softMinimum': round(soft_min, 2),
                        'softMaximum': round(soft_max, 2),
                        'softMinimumEnabled': True,
                        'softMaximumEnabled': True
                    }
                )
                self.rules.append(rule)
                print(f"  ✅ Created: Statistical range rule ({soft_min:.2f} to {soft_max:.2f})")
    
    def _generate_string_rules(self):
        """Generate rules for string columns"""
        print("\n" + "="*80)
        print("GENERATING STRING COLUMN RULES")
        print("="*80)
        
        for col_name in self.profile['categorization']['string']:
            col_profile = next(c for c in self.profile['columns'] if c['name'] == col_name)
            
            print(f"\n📝 {col_name}")
            
            # Length validation (if we have stats)
            length_stats = col_profile.get('length_stats', {})
            if length_stats:
                min_len = length_stats.get('min', 0)
                max_len = length_stats.get('max', 1000)
                
                # Add some tolerance
                soft_max = max(int(max_len * 1.5), max_len + 10)
                
                rule = self._create_rule_config(
                    rule_type='ColumnLengthInRangeRule',
                    name=f"{col_name}_length",
                    params={
                        'column': col_name,
                        'softMaximum': soft_max,
                        'softMaximumEnabled': True
                    }
                )
                self.rules.append(rule)
                print(f"  ✅ Created: Length rule (max {soft_max} chars)")
    
    def _generate_date_rules(self):
        """Generate rules for date columns"""
        print("\n" + "="*80)
        print("GENERATING DATE COLUMN RULES")
        print("="*80)
        
        for col_name in self.profile['categorization']['date']:
            col_profile = next(c for c in self.profile['columns'] if c['name'] == col_name)
            
            print(f"\n📅 {col_name}")
            
            # Not in future rule
            rule = self._create_rule_config(
                rule_type='ColumnDateNotInFutureRule',
                name=f"{col_name}_not_future",
                params={'column': col_name},
                severity='ERROR'
            )
            self.rules.append(rule)
            print(f"  ✅ Created: Not-in-future rule")
            
            # Reasonable range rule
            date_range = col_profile.get('date_range', {})
            if date_range:
                rule = self._create_rule_config(
                    rule_type='ColumnValueInRangeRule',
                    name=f"{col_name}_reasonable_range",
                    params={
                        'column': col_name,
                        'softMinimum': date_range.get('min'),
                        'softMaximum': date_range.get('max'),
                        'softMinimumEnabled': True,
                        'softMaximumEnabled': True
                    }
                )
                self.rules.append(rule)
                print(f"  ✅ Created: Date range rule")
    
    def _generate_nullability_rules(self):
        """Generate not-null rules"""
        print("\n" + "="*80)
        print("GENERATING NULLABILITY RULES")
        print("="*80)
        
        for col_profile in self.profile['columns']:
            col_name = col_profile['name']
            null_ratio = col_profile.get('null_ratio', 0)
            
            # Only create not-null rule if null ratio is low
            if null_ratio < 0.05:  # Less than 5% nulls
                rule = self._create_rule_config(
                    rule_type='ColumnValuesNotEmptyRule',
                    name=f"{col_name}_not_null",
                    params={'column': col_name, 'thresholdType': 'all'},
                    severity='ERROR'
                )
                self.rules.append(rule)
        
        print(f"✅ Created {len([r for r in self.rules if 'not_null' in r['name']])} not-null rules")
    
    def _generate_uniqueness_rules(self):
        """Generate uniqueness rules"""
        print("\n" + "="*80)
        print("GENERATING UNIQUENESS RULES")
        print("="*80)
        
        for col_profile in self.profile['columns']:
            col_name = col_profile['name']
            unique_ratio = col_profile.get('unique_ratio', 0)
            
            # Create uniqueness rule if column appears to be a unique identifier
            if unique_ratio > 0.95:
                rule = self._create_rule_config(
                    rule_type='ColumnValuesUniqueRule',
                    name=f"{col_name}_unique",
                    params={'column': col_name},
                    severity='ERROR'
                )
                self.rules.append(rule)
                print(f"✅ Created uniqueness rule for: {col_name}")
    
    def _generate_dataset_level_rules(self):
        """Generate dataset-level rules"""
        print("\n" + "="*80)
        print("GENERATING DATASET-LEVEL RULES")
        print("="*80)
        
        sample_size = self.profile['sample_size']
        
        # Record count stability
        soft_min = int(sample_size * 0.8)
        soft_max = int(sample_size * 1.5)
        
        rule = self._create_rule_config(
            rule_type='RecordCountInRangeRule',
            name='record_count_stability',
            params={
                'softMinimum': soft_min,
                'softMaximum': soft_max,
                'softMinimumEnabled': True,
                'softMaximumEnabled': True
            }
        )
        self.rules.append(rule)
        print(f"✅ Created: Record count stability rule")
        
        # Column count stability
        col_count = self.profile['total_columns']
        
        rule = self._create_rule_config(
            rule_type='ColumnCountInRangeRule',
            name='column_count_stability',
            params={
                'minimum': col_count,
                'maximum': col_count,
                'minimumEnabled': True,
                'maximumEnabled': True
            },
            severity='ERROR'
        )
        self.rules.append(rule)
        print(f"✅ Created: Column count stability rule")
