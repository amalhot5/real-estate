Pre-reqs to run this script:
- [data/ folder](https://drive.google.com/file/d/1VayQEMG6d9jwonLbfLJyBPdwCiD2fw6l/view?usp=sharing) to be placed in the same directory as the scripts
- [miniconda](https://docs.anaconda.com/miniconda/) or python v3.10+
- create conda virturalenv using `env_specs.txt` or the following commands (can also be done with `pip`):
    - `conda create -n street_search`
    - `conda activate street_search`
    - `conda install -c conda-forge pandas`
    - `conda install -c conda-forge geopandas`
    - `conda install -c conda-forge tqdm`

This folder contains the scripts needed to run the buildings to streets matching
needed for street search:
- nyc_building_geos.sql: query to get nyc buildings and their geographies
- buildings_to_streets.py: creates buildings to streets assoications
    - specify buildings dataset in command line arguments e.g. `python buildings_to_streets data/nyc_all_geos.csv`
- street_intersections.py: creates streets from street segments and street intersection table

Run the two python scripts once you have created and activated the conda/python virtualenv. The output of the scripts will be:
- `data/buildings_x_streets.csv`: a table with all the buildings and their matched streets
- `data/display_streets.csv`: a table with all the streets, their geographies, and display_names
- `data/street_intersections.csv`: a table containing all the intersections for each street
- `data/umatched_round2_buildings.html`: a map of all unmatched buildings
- `data/corner_buildings_fals_positive_check.html`: a map to check the buildings matched to more than one street
