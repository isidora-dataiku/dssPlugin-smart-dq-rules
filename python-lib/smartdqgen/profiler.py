"""
Dataset Profiler Module
Analyzes dataset structure, statistics, and patterns to inform rule generation
"""

import dataiku
import pandas as pd
from typing import Dict, Optional


class DatasetProfiler:
    """Profiles a dataset to extract statistics and patterns for rule generation"""
    
    def __init__(self, project_key: str, dataset_name: str, sample_size: int = 10000):
        """
        Initialize the profiler
        
        Args:
            project_key: DSS project key
            dataset_name: Name of the dataset to profile
            sample_size: Number of rows to sample for profiling
        """
        self.project_key = project_key
        self.dataset_name = dataset_name
        self.sample_size = sample_size
        
        # Get dataset handle
        self.dataset = dataiku.Dataset(dataset_name, project_key=project_key)
        
    def profile_dataset(self) -> Optional[Dict]:
        """
        Profile the dataset and return comprehensive statistics.
        Relies on Pandas type inference for the profile.
        
        Returns:
            Dictionary containing dataset profile, or error dict if profiling fails
        """
        try:
            print(f"📊 Profiling dataset: {self.dataset_name}")
            print(f"   Sample size: {self.sample_size:,} rows")
            
            # MEMORY SAFETY: Check column count before loading
            # We use read_schema() just to get the column list efficiently
            schema = self.dataset.read_schema()
            all_columns = [col['name'] for col in schema]
            total_columns = len(all_columns)
            
            # Limit to 500 columns to prevent Out Of Memory errors
            MAX_COLUMNS = 500
            
            if total_columns > MAX_COLUMNS:
                print(f"⚠️ Dataset has {total_columns} columns. Profiling first {MAX_COLUMNS} only.")
                cols_to_profile = all_columns[:MAX_COLUMNS]
                # infer_with_pandas=True ensures we get native python types
                df = self.dataset.get_dataframe(limit=self.sample_size, columns=cols_to_profile, infer_with_pandas=True)
            else:
                df = self.dataset.get_dataframe(limit=self.sample_size, infer_with_pandas=True)
            
            if df.empty:
                print("⚠️ Dataset is empty")
                return {
                    'error': True,
                    'error_message': 'Dataset is empty',
                    'dataset_name': self.dataset_name
                }
            
            print(f"✅ Loaded {len(df):,} rows, {len(df.columns)} columns")
            
            # Build comprehensive profile
            profile = {
                'dataset_name': self.dataset_name,
                'sample_size': len(df),
                'total_columns': len(df.columns),
                'columns': [],
                'categorization': {
                    'categorical': [],
                    'numeric': [],
                    'string': [],
                    'date': [],
                    'boolean': []
                }
            }
            
            # Profile each column
            for col_name in df.columns:
                col_profile = self._profile_column(df, col_name)
                profile['columns'].append(col_profile)
                
                # Add to categorization lists
                if col_profile.get('type'):
                    profile['categorization'][col_profile['type']].append(col_name)
            
            # Print Summary
            print(f"\n📈 Profile Summary:")
            for cat, cols in profile['categorization'].items():
                print(f"   {cat.capitalize()}: {len(cols)} columns")
            
            return profile
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"❌ Error profiling dataset: {e}")
            print(error_details)
            
            return {
                'error': True,
                'error_message': str(e),
                'error_traceback': error_details,
                'dataset_name': self.dataset_name
            }
    
    def _profile_column(self, df: pd.DataFrame, col_name: str) -> Dict:
        """Profile a single column"""
        col_data = df[col_name]
        
        profile = {
            'name': col_name,
            'null_count': int(col_data.isnull().sum()),
            'null_ratio': float(col_data.isnull().sum() / len(col_data)),
            'unique_count': int(col_data.nunique()),
            'unique_ratio': float(col_data.nunique() / len(col_data)),
            'sample_values': col_data.dropna().head(5).tolist()
        }
        
        # LOGIC: Check types in order of specificity
        
        # 1. Boolean
        if pd.api.types.is_bool_dtype(col_data) or (col_data.dropna().isin([0, 1, True, False]).all() and len(col_data.dropna()) > 0 and not pd.api.types.is_float_dtype(col_data)):
            profile['type'] = 'boolean'
            profile.update(self._profile_categorical(col_data))
            
        # 2. Date/Time
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            profile['type'] = 'date'
            profile.update(self._profile_date(col_data))
            
        # 3. Numeric
        elif pd.api.types.is_numeric_dtype(col_data):
            profile['type'] = 'numeric'
            profile.update(self._profile_numeric(col_data))
            
        # 4. String / Categorical fallback
        else:
            # Heuristic: Low cardinality relative to data size = Categorical
            if profile['unique_ratio'] < 0.05 or profile['unique_count'] <= 20:
                profile['type'] = 'categorical'
                profile.update(self._profile_categorical(col_data))
            else:
                profile['type'] = 'string'
                profile.update(self._profile_string(col_data))
        
        return profile
    
    def _profile_numeric(self, series: pd.Series) -> Dict:
        """Profile numeric column"""
        clean_series = series.dropna()
        if len(clean_series) == 0: return {}
        
        return {
            'stats': {
                'min': float(clean_series.min()),
                'max': float(clean_series.max()),
                'mean': float(clean_series.mean()),
                'median': float(clean_series.median()),
                'std': float(clean_series.std()),
                'q25': float(clean_series.quantile(0.25)),
                'q75': float(clean_series.quantile(0.75))
            },
            'all_non_negative': bool((clean_series >= 0).all()),
            'all_positive': bool((clean_series > 0).all()),
            'all_integers': bool((clean_series == clean_series.astype(int)).all())
        }
    
    def _profile_categorical(self, series: pd.Series) -> Dict:
        """Profile categorical column"""
        value_counts = series.value_counts()
        return {
            'cardinality': int(len(value_counts)),
            'top_values': value_counts.head(10).to_dict(),
            'most_frequent_value': str(value_counts.index[0]) if len(value_counts) > 0 else None
        }
    
    def _profile_string(self, series: pd.Series) -> Dict:
        """Profile string column"""
        clean_series = series.dropna().astype(str)
        if len(clean_series) == 0: return {}
        lengths = clean_series.str.len()
        return {
            'length_stats': {
                'min': int(lengths.min()),
                'max': int(lengths.max()),
                'mean': float(lengths.mean()),
                'median': float(lengths.median())
            }
        }
    
    def _profile_date(self, series: pd.Series) -> Dict:
        """Profile date column"""
        clean_series = series.dropna()
        if len(clean_series) == 0: return {}
        return {
            'date_range': {
                'min': str(clean_series.min()),
                'max': str(clean_series.max())
            }
        }