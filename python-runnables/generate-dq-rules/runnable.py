"""
Smart Data Quality Rules Generator - Main Runnable
Automatically generates intelligent data quality rules based on dataset profiling
"""

import json
import time
from datetime import datetime

import dataiku
from dataiku.runnables import Runnable
from dataikuapi.utils import DataikuException

# Import our library modules
from smartdqgen.profiler import DatasetProfiler
from smartdqgen.rule_generator import RuleGenerator
from smartdqgen.api_client import DataQualityAPIClient


class MyRunnable(Runnable):
    """Smart Data Quality Rules Generator Macro"""

    def __init__(self, project_key, config, plugin_config):
        """
        Initialize the runnable
        """
        print('Smart DQ Rules Generator - Initializing')
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
        # Parse configuration
        self.dataset_selection_mode = self.config.get("dataset_selection_mode", "single")
        self.sample_size = self.config.get("sample_size", 10000)
        self.strictness = self.config.get("strictness", "balanced")
        self.enable_rules = self.config.get("enable_rules", True)
        self.skip_existing = self.config.get("skip_existing", True)
        
        # Initialize API client
        self.client = dataiku.api_client()
        self.project = self.client.get_project(project_key)
        
        # Determine which datasets to process
        self.dataset_names = []
        
        if self.dataset_selection_mode == "single":
            single_dataset = self.config.get("input_dataset")
            if single_dataset:
                self.dataset_names = [single_dataset]
        elif self.dataset_selection_mode == "multiple":
            multiple_datasets = self.config.get("input_datasets", [])
            self.dataset_names = multiple_datasets if isinstance(multiple_datasets, list) else [multiple_datasets]
        elif self.dataset_selection_mode == "all":
            # Get all datasets in the project
            all_datasets = self.project.list_datasets()
            self.dataset_names = [ds['name'] for ds in all_datasets]
        
        if not self.dataset_names:
            raise ValueError("No datasets selected. Please select at least one dataset.")
        
        print(f'Configuration:')
        print(f'  Mode: {self.dataset_selection_mode}')
        print(f'  Datasets: {len(self.dataset_names)} dataset(s)')
        print(f'  Sample size: {self.sample_size}')
        print(f'  Strictness: {self.strictness}')
        print(f'  Enable rules: {self.enable_rules}')

    def get_progress_target(self):
        """
        Define progress tracking
        Returns (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        # 3 phases per dataset
        return (len(self.dataset_names) * 3, 'RECORDS')

    def run(self, progress_callback):
            """
            Main execution logic
            """
            start_time = time.time()
            progress_counter = 0

            # Store results for all datasets
            all_dataset_results = []

            try:
                # Process each dataset
                # Enumerate starts at 1 so we can easily calculate targets (e.g., 1 * 3 = 3)
                for dataset_idx, dataset_name in enumerate(self.dataset_names, 1):
                    print("\n" + "="*80)
                    print(f"PROCESSING DATASET {dataset_idx}/{len(self.dataset_names)}: {dataset_name}")
                    print("="*80)

                    dataset_result = {
                        'dataset_name': dataset_name,
                        'success': False,
                        'error': None,
                        'profile': None,
                        'generated_rules': [],
                        'creation_results': {}
                    }

                    try:
                        # PHASE 1: Dataset Profiling
                        print(f"Phase 1: Profiling {dataset_name}...")

                        profiler = DatasetProfiler(
                            project_key=self.project_key,
                            dataset_name=dataset_name,
                            sample_size=self.sample_size
                        )

                        profile = profiler.profile_dataset()

                        if not profile or profile.get('error'):
                            error_msg = "Failed to profile dataset"
                            if profile and profile.get('error_message'):
                                error_msg = f"{error_msg}: {profile.get('error_message')}"

                            dataset_result['error'] = error_msg
                            all_dataset_results.append(dataset_result)
                            progress_counter = dataset_idx * 3
                            progress_callback(progress_counter)
                            continue

                        dataset_result['profile'] = profile
                        progress_counter += 1
                        progress_callback(progress_counter)

                        # PHASE 2: Rule Generation
                        print(f"Phase 2: Generating rules for {dataset_name}...")

                        generator = RuleGenerator(
                            profile=profile,
                            strictness=self.strictness,
                            enable_rules=self.enable_rules
                        )

                        generated_rules = generator.generate_all_rules()

                        if not generated_rules:
                            dataset_result['error'] = "No rules were generated"
                            all_dataset_results.append(dataset_result)
                            # FIX: Jump directly to the end of this dataset's progress block
                            progress_counter = dataset_idx * 3
                            progress_callback(progress_counter)
                            continue

                        dataset_result['generated_rules'] = generated_rules
                        print(f"Generated {len(generated_rules)} rules for {dataset_name}")
                        progress_counter += 1
                        progress_callback(progress_counter)

                        # PHASE 3: API Integration - Create Rules
                        print(f"Phase 3: Creating rules for {dataset_name}...")

                        api_client = DataQualityAPIClient(
                            project=self.project,
                            dataset_name=dataset_name,
                            skip_existing=self.skip_existing
                        )

                        results = api_client.create_rules(generated_rules)

                        dataset_result['creation_results'] = results
                        dataset_result['success'] = True
                        progress_counter += 1
                        progress_callback(progress_counter)

                    except Exception as e:
                        import traceback
                        error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
                        print(f"Error processing {dataset_name}: {error_msg}")
                        dataset_result['error'] = error_msg

                        # FIX: ROBUST PROGRESS CALCULATION
                        # Instead of calculating modulo, we simply force the counter 
                        # to the value it SHOULD have at the end of this dataset.
                        # Each dataset has 3 steps. At the end of dataset 1, we should be at 3.
                        # At the end of dataset 2, we should be at 6.
                        progress_counter = dataset_idx * 3 
                        progress_callback(progress_counter)

                    all_dataset_results.append(dataset_result)

                # Generate consolidated HTML Report
                elapsed_time = time.time() - start_time
                html_report = self._generate_multi_dataset_html_report(
                    all_dataset_results=all_dataset_results,
                    elapsed_time=elapsed_time
                )

                return html_report

            except Exception as e:
                import traceback
                error_msg = f"Error: {str(e)}\n\n{traceback.format_exc()}"
                print(error_msg)
                return self._generate_error_html(error_msg)
        
    def _generate_multi_dataset_html_report(self, all_dataset_results, elapsed_time):
        """Generate an HTML report for multiple datasets"""

        # Calculate overall statistics
        total_datasets = len(all_dataset_results)
        successful_datasets = len([r for r in all_dataset_results if r['success']])
        failed_datasets = total_datasets - successful_datasets

        total_rules_generated = sum(len(r['generated_rules']) for r in all_dataset_results)
        total_rules_created = sum(len(r['creation_results'].get('created', [])) for r in all_dataset_results)
        total_rules_skipped = sum(len(r['creation_results'].get('skipped', [])) for r in all_dataset_results)

        overall_success_rate = (total_rules_created / total_rules_generated * 100) if total_rules_generated > 0 else 0

        # Build HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                    font-size: 14px;
                    line-height: 1.5;
                    color: #333;
                    background-color: #fff;
                    margin: 0;
                    padding: 20px;
                }}
                .container {{
                    max-width: 100%;
                }}
                h1 {{
                    font-size: 18px;
                    font-weight: 600;
                    margin: 0 0 5px 0;
                    color: #222;
                }}
                .header-meta {{
                    color: #666;
                    margin-bottom: 20px;
                    padding-bottom: 20px;
                    border-bottom: 1px solid #eee;
                }}

                /* Info Box */
                .info-box {{
                    background: #f0f8ff;
                    border-left: 4px solid #2E7975;
                    padding: 15px;
                    margin-bottom: 25px;
                    font-size: 13px;
                    color: #555;
                }}
                .info-box strong {{
                    color: #222;
                }}

                /* Simple Stats Grid */
                .stats-container {{
                    display: flex;
                    flex-wrap: wrap;
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                .stat-box {{
                    border: 1px solid #ddd;
                    padding: 15px;
                    min-width: 140px;
                    flex: 1;
                }}
                .stat-label {{
                    font-size: 12px;
                    color: #666;
                    text-transform: uppercase;
                    margin-bottom: 5px;
                }}
                .stat-value {{
                    font-size: 24px;
                    font-weight: 600;
                    color: #222;
                }}
                .stat-helper {{
                    font-size: 11px;
                    color: #888;
                    margin-top: 5px;
                }}
                .text-teal {{ color: #2E7975; }}

                /* Dataset List */
                .dataset-row {{
                    border: 1px solid #ddd;
                    margin-bottom: 15px;
                    padding: 15px;
                    background: #f9f9f9;
                }}
                .dataset-header {{
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 10px;
                }}
                .dataset-title {{
                    font-weight: 600;
                    font-size: 16px;
                }}
                .badge {{
                    padding: 2px 8px;
                    font-size: 11px;
                    border-radius: 2px;
                    font-weight: 600;
                }}
                .badge-success {{ background: #e0f2f1; color: #00695c; }}
                .badge-error {{ background: #ffebee; color: #c62828; }}

                .details {{
                    font-size: 13px;
                    color: #555;
                    margin-bottom: 10px;
                }}

                /* The Button */
                .btn {{
                    display: inline-block;
                    padding: 6px 12px;
                    background-color: #2E7975;
                    color: white;
                    text-decoration: none;
                    font-weight: 500;
                    font-size: 13px;
                    border-radius: 3px;
                }}
                .btn:hover {{
                    background-color: #1F524F;
                }}

                .error-text {{
                    font-family: monospace;
                    background: #fff;
                    padding: 10px;
                    border: 1px solid #ffcdd2;
                    color: #b71c1c;
                    white-space: pre-wrap;
                }}

                .next-steps {{
                    background: #f9f9f9;
                    border: 1px solid #ddd;
                    padding: 15px;
                    margin-top: 25px;
                }}
                .next-steps h3 {{
                    font-size: 14px;
                    margin-top: 0;
                    color: #222;
                }}
                .next-steps ol {{
                    margin: 10px 0 0 0;
                    padding-left: 20px;
                }}
                .next-steps li {{
                    margin-bottom: 5px;
                    font-size: 13px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Smart Data Quality Rules - Generation Report</h1>
                <div class="header-meta">
                    Project: <strong>{self.project_key}</strong> &bull; 
                    Generated in {elapsed_time:.1f}s &bull;
                    Processed {total_datasets} dataset{"s" if total_datasets != 1 else ""}
                </div>

                <div class="info-box">
                    <strong>What this report shows:</strong> This macro analyzed your datasets and automatically created data quality rules. 
                    The "Rule Creation Success Rate" below shows what percentage of generated rules were successfully added to Dataiku. 
                    <strong>Next step:</strong> Click "View Data Quality" for each dataset, then click "Compute All" to actually run the rules.
                </div>

                <div class="stats-container">
                    <div class="stat-box">
                        <div class="stat-label">Rule Creation Success Rate</div>
                        <div class="stat-value text-teal">{overall_success_rate:.1f}%</div>
                        <div class="stat-helper">{total_rules_created} of {total_rules_generated} rules created</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">New Rules Created</div>
                        <div class="stat-value">{total_rules_created}</div>
                        <div class="stat-helper">Ready to compute</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Datasets Processed</div>
                        <div class="stat-value">{successful_datasets} / {total_datasets}</div>
                        <div class="stat-helper">{"All" if failed_datasets == 0 else f"{failed_datasets}"} {"succeeded" if failed_datasets == 0 else "failed"}</div>
                    </div>
                </div>

                <h2 style="font-size: 16px; margin-bottom: 15px;">Dataset-by-Dataset Results</h2>
        """

        # Add each dataset result
        # Add each dataset result
        for result in all_dataset_results:
            dataset_name = result['dataset_name']  # REMOVED html.escape()
            success = result['success']

            # NOTE: This uses a relative path which works on any DSS instance
            # target="_top" ensures it opens in the main window, not the iframe
            dq_link = f"/projects/{self.project_key}/datasets/{result['dataset_name']}/data-quality/current-status"

            html += f"""
                <div class="dataset-row">
                    <div class="dataset-header">
                        <span class="dataset-title">{dataset_name}</span>
                        <span class="badge {'badge-success' if success else 'badge-error'}">
                            {'RULES CREATED' if success else 'FAILED'}
                        </span>
                    </div>
            """

            if success:
                created = len(result['creation_results'].get('created', []))
                skipped = len(result['creation_results'].get('skipped', []))
                unsupported = len(result['creation_results'].get('unsupported', []))

                html += f"""
                    <div class="details">
                        <strong>{created} new rules</strong> added to this dataset
                        {f" &bull; {skipped} skipped (already exist)" if skipped > 0 else ""}
                        {f" &bull; {unsupported} unsupported rule types" if unsupported > 0 else ""}
                    </div>
                    <a href="{dq_link}" target="_top" class="btn">View Data Quality</a>
                """
            else:
                error_msg = result.get('error', 'Unknown error')[:500]  # REMOVED html.escape()
                html += f"""
                    <div class="details" style="color: #c62828; font-weight: 500;">
                        Failed to generate rules for this dataset
                    </div>
                    <div class="error-text">{error_msg}</div>
                """

            html += "</div>"

        html += f"""
                <div class="next-steps">
                    <h3>Next Steps</h3>
                    <ol>
                        <li>Click <strong>"View Data Quality"</strong> for each dataset above</li>
                        <li>Review the generated rules in the Data Quality tab</li>
                        <li>Click <strong>"Compute All"</strong> to run the rules and see results</li>
                        <li>Rules flagged as violations will show in red - investigate these records</li>
                        <li>Adjust rule thresholds if needed, then re-compute</li>
                    </ol>
                </div>
            </div>
        </body>
        </html>
        """

        return html

    def _generate_error_html(self, error_message):
        """Generate an error HTML page"""
        return f"""
        <!DOCTYPE html>
        <html>
        <body style="font-family: sans-serif; padding: 20px; color: #333;">
            <h1 style="color: #c62828;">Generation Failed</h1>
            <p>An error occurred while generating rules:</p>
            <pre style="background: #f5f5f5; padding: 10px; border: 1px solid #ddd;">{error_message}</pre>
        </body>
        </html>
        """