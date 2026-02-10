# PEDP scripts

This is not your normal GitHub repository.

Instead of containing a single project with a coherent structure, 
it is intended to allow any coders who are:
- archiving data sets for the Public Environmental Data Project 
(https://www.openenvironmentaldata.org/pilot-type/public-environmental-data-project) OR
- building utilities for audit/maintenance of the Dataverse
to upload their working scripts both for safe-keeping and for
others to borrow from.

Eventually, the code in this repo may evolve into more coherent structure
of tools. For now, however, 
- small scraping/archiving code should simply be added in 
folders under _/oneTimeHacks_ and
- audit/management scripts should be added under _/utilities/audit_

## Adding code
Follow the steps in the [Contributing guide](/CONTRIBUTING.md), specifically for making [code changes](https://github.com/Public-Environmental-Data-Partners/overview/blob/main/CONTRIBUTING.md#code-and-documentation-changes) using Pull Requests.

### oneTimeHacks
The `oneTimeHacks` folder is for code you ran for one archiving project and don't expect to run again. If you add code to the `oneTimeHacks` folder:
- create a folder for your code, use your own best judgement for a name
- along with any working code, add a _readme.txt_ file that documents:
    - the URL of the primary web page you were archiving
    - the URL of the Dataverse (or other online repository) where your archived data was stored
    - any other notes you want to make about the code you are archiving
	
### utilities
The `utilities` folder is for reusable code. For now, there's only one folder in this group:
- audit
Add other subfolders as needed and please include a readme.txt file 
for each folder you create.
