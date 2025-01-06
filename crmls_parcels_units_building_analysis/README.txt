PRD linked here: 
https://www.notion.so/CRMLS-Parcels-Units-Building-Analysis-e8f649268ead41f5869d732effb50484?pvs=4

The goal of this project is to develop a methodology to match building units to their overall building.
This is especially a problem for building complexes. These complexes have multiple buildings on them,
but all have the same address. The purpose of this methodology is to properly identify the buildings 
that are on the same address as different units of the overall building complex.

A sample of CRMLS buildings units and their potential overall building complexes was provided by Data Engineering:
    1. crmls_units.csv - building units dataset
    2. crmls_potential_unit_buildings.csv - potential building complexes

The files in the project are:
    - data/: folder containing datasets (original input and temporarily created ones)
    - data.ipynb: read and clean data provided by DE 
    - exploratory_data_analysis.ipynb: exploring data
    - test_methodology.ipynb: testing out different merges & methodologies
    - final_methodolgy.ipynb: prototyping final merge and generating new buildings
    - building_complex.py: final methodology chosen
    - requirements.yml: python packages used