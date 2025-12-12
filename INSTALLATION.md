# Installation Guide

## Prerequisites

Before installing the Smart Data Quality plugin, ensure you have:

* Dataiku DSS version 12.0 or higher
* Python 3.7+ code environment
* Project-level or instance-level admin permissions
* For Agent Tool: Access to create and configure AI agents

## Installation Steps

1. Download the latest release from [GitHub releases](https://github.com/ckwonc/dssPlugin-smart-dq-rules/releases)
2. In your Dataiku instance, navigate to **Plugins** > **Add Plugin** > **Upload**
3. Select the downloaded `.zip` file
4. Choose installation scope (instance-level or project-level)
5. Click **Install**

## Post-Installation Configuration

### Setting Up the Macro

The macro is automatically available after installation. No additional configuration is required.

To verify:
1. Open any Dataiku project
2. Select a dataset
3. Check the Actions menu for "Generate Data Quality Rules"

### Setting Up the Agent Tool

1. **Navigate to Agent Tools**:
   * Open any Dataiku project
   * Click the **GenAI** dropdown in the top left corner
   * Select **Agent Tools**

2. **Add the Tool**:
   * Click **+ Agent Tool**
   * Select **Create Business Rules** from the list

3. **Configure the Tool**:
   * **Dataset**: Select the target dataset for rule creation
   * **Description** (optional): Add context about the tool's purpose
   * Click **Save**

4. **Create or Update an Agent**:
   * Go to **GenAI** > **Agents**
   * Create a new agent or edit an existing one
   * In the agent configuration, add the "Create Business Rules" tool
   * Save the agent

## Code Environment

The plugin uses Dataiku's built-in code environment. No additional configuration is required.

## Troubleshooting Installation

### Plugin Won't Install

**Issue**: Error message during plugin upload

**Solutions**:
* Verify you have admin permissions
* Check that the `.zip` file is not corrupted
* Ensure DSS version is 12.0 or higher
* Review DSS logs for specific error messages

### Macro Not Appearing

**Issue**: "Generate Data Quality Rules" not visible in Actions menu

**Solutions**:
* Refresh the project page
* Verify plugin is installed (check Plugins page)
* Ensure you have proper permissions for the dataset
* Try reinstalling the plugin

### Agent Tool Not Available

**Issue**: "Create Business Rules" not appearing in agent tool list

**Solutions**:
* Verify plugin is installed at the correct scope
* Check that GenAI features are enabled in your DSS instance
* Ensure you have permissions to create agent tools
* Restart the DSS instance if necessary

### Code Environment Errors

**Issue**: Import errors or missing packages

**Solutions**:
* Update the plugin's code environment
* Install missing dependencies manually
* Check Python version compatibility (3.7+)
* Review code environment logs in DSS

## Uninstallation

To remove the plugin:

1. Navigate to **Plugins**
2. Find "Smart Data Quality"
3. Click the settings icon
4. Select **Uninstall**
5. Confirm removal

**Note**: Uninstalling the plugin does not remove existing data quality rules. Rules created by the plugin will remain active.

## Upgrading

To upgrade to a newer version:

1. Download the latest release
2. Navigate to **Plugins** > Smart Data Quality > Settings
3. Click **Replace with ZIP**
4. Upload the new version
5. Confirm replacement

Existing rules and configurations will be preserved during upgrades.
