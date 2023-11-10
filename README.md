# Researcher_Profiler
This script scrapes a locally saved static pull of the OpenAlex database of 75M+ researchers to extract a select group of users that meet filtering criteria including citation count, ORCID documentation, expertise tags, and more.

**Input/Usage:**
`extract_and_save("Olfaction")`

**Output:**
File: `Olfaction_0.xlsx`

Here is an example output of the script when prompted with the keyword "Olfaction" as well as using Filters (hardcoded in the script) for 5-1000 citation count, ORCID active, published within the past 5 years, and matching the relevant expertise.
![image](https://github.com/TylerDiorio/Researcher_Profiler/assets/109099227/f2f98e4d-e673-4698-b09f-424724a9db19)

**Requirements**: Access to the OpenAlex Snapshot of researchers - currently this is saved locally as visible in line 246 as my local D:// drive path, however this can be adapted on any storage system.
