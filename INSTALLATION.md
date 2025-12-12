# Installation and Testing Guide

## 📦 Plugin Structure

```
smart-dq-rules-generator/
├── plugin.json                          # Plugin metadata
├── README.md                            # User documentation
├── python-runnables/
│   └── generate-dq-rules/
│       ├── runnable.json                # Macro UI configuration
│       └── runnable.py                  # Main execution logic
└── python-lib/
    └── smartdqgen/
        ├── __init__.py                  # Package init
        ├── profiler.py                  # Phase 1: Dataset profiling
        ├── rule_generator.py            # Phase 2: Rule generation
        └── api_client.py                # Phase 3: API integration
```

## 🚀 Installation Steps

### Method 1: Install from ZIP (Recommended)

1. **Create the plugin ZIP file**:
   ```bash
   cd /home/claude
   zip -r smart-dq-rules-generator.zip smart-dq-rules-generator/
   ```

2. **Upload to Dataiku**:
   - Go to **Administration** > **Plugins**
   - Click **+ Add Plugin** > **Upload**
   - Select `smart-dq-rules-generator.zip`
   - Click **Install**

3. **Verify Installation**:
   - Plugin should appear in the plugins list
   - Status should be "Loaded"

### Method 2: Install from Dev Repository

1. **Clone/Copy to Dataiku**:
   ```bash
   # Copy entire directory to Dataiku's plugin development folder
   cp -r smart-dq-rules-generator/ $DATAIKU_HOME/plugins/dev/
   ```

2. **Reload Plugins**:
   - Go to **Administration** > **Plugins**
   - Click **Reload** (development mode)

## 🧪 Testing the Plugin

### Test Dataset Setup

Use your existing `real_estate_sales_S3` dataset (converted to SQL):

```sql
-- Dataset should have:
-- - Categorical columns: property_type
-- - Numeric columns: transaction_id, property_price, living_surface
-- - String columns: address
-- - Date columns: transaction_date
-- Approximately 10,000 rows
```

### Test Case 1: Basic Functionality

1. **Navigate to your SQL dataset**
2. **Click Actions (⋮) > Macros > Generate Smart Data Quality Rules**
3. **Configure**:
   - Sample size: 10000
   - Strictness: Balanced
   - Enable rules: ✓
   - Skip existing: ✓
4. **Click Run**
5. **Expected Results**:
   - HTML report shows 90%+ success rate
   - ~50 rules created
   - Processing time: 10-20 seconds

### Test Case 2: Different Strictness Levels

**Test Lenient**:
- Sample size: 10000
- Strictness: Lenient
- Expect: Wider ranges, fewer warnings

**Test Strict**:
- Sample size: 10000
- Strictness: Strict
- Expect: Tighter ranges, more warnings

### Test Case 3: Skip Existing Rules

1. **Run macro first time** - creates rules
2. **Run macro second time** with "Skip existing" checked
3. **Expected**: All rules skipped, no duplicates

### Test Case 4: Small Sample Size

1. **Configure**:
   - Sample size: 1000
   - Strictness: Balanced
2. **Expected**: Faster execution (<5 seconds), less accurate thresholds

## ✅ Validation Checklist

After installation, verify:

- [ ] Plugin appears in Plugins list
- [ ] Macro appears in dataset Actions menu
- [ ] Macro appears when right-clicking dataset in Flow
- [ ] Macro runs without Python import errors
- [ ] HTML report generates successfully
- [ ] Rules appear in Data Quality tab
- [ ] Rules can be computed successfully
- [ ] Rules show OK/Warning/Error statuses

## 🐛 Troubleshooting

### Common Issues

**Issue**: "Module 'smartdqgen' not found"
- **Solution**: Verify `python-lib/smartdqgen/__init__.py` exists
- Check file permissions

**Issue**: "Dataset cannot be read"
- **Solution**: Verify dataset is SQL-based, not S3/Spark
- Check dataset permissions

**Issue**: "Rules fail to compute"
- **Solution**: S3/Spark datasets only support dataset-level rules
- Switch to SQL dataset for full functionality

**Issue**: "High failure rate (>20%)"
- **Solution**: Check Dataiku version (requires DSS 12+)
- Verify Data Quality API is enabled

**Issue**: "Timeout errors"
- **Solution**: Reduce sample size
- Profile fewer columns

### Debug Mode

To enable detailed logging, add to `runnable.py`:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 📊 Expected Performance

### Typical Results (10,000 row dataset, 14 columns):

| Metric | Value |
|--------|-------|
| **Profiling Time** | 1-2 seconds |
| **Rule Generation** | <1 second |
| **API Creation** | 5-10 seconds |
| **Total Time** | 10-15 seconds |
| **Rules Generated** | 50-54 rules |
| **Success Rate** | 90-96% |

### By Rule Type:

| Rule Type | Count |
|-----------|-------|
| Numeric ranges | 20-30 |
| Not-null checks | 10-15 |
| Categorical | 3-5 |
| Uniqueness | 1-2 |
| Dataset-level | 2 |
| Custom Python | 0-2 |

## 🔧 Configuration Options

### Adjust in runnable.json:

```json
{
  "sample_size": {
    "defaultValue": 10000,  // Change default sample size
  },
  "strictness": {
    "defaultValue": "balanced"  // Change default strictness
  }
}
```

### Adjust in rule_generator.py:

```python
# Sigma multipliers for strictness levels
self.sigma_multipliers = {
    'lenient': 5.0,   // Increase for wider ranges
    'balanced': 3.0,  // Standard setting
    'strict': 2.0     // Decrease for tighter ranges
}
```

## 📝 Next Steps

After successful installation and testing:

1. **Document** in your team wiki
2. **Train** users on the macro
3. **Establish** best practices for your organization
4. **Monitor** rule effectiveness over time
5. **Iterate** based on feedback

## 🆘 Support

For help:
- Check Dataiku logs: `$DATAIKU_HOME/run/backend.log`
- Review plugin code in dev editor
- Contact plugin maintainer
- Open GitHub issue

## 📚 Additional Resources

- [Dataiku Plugin Documentation](https://doc.dataiku.com/dss/latest/plugins/)
- [Data Quality API Reference](https://developer.dataiku.com/latest/api-reference/)
- [Plugin Development Tutorial](https://developer.dataiku.com/latest/tutorials/plugins/)
