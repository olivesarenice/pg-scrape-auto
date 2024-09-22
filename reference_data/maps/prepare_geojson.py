import geopandas as gpd
import pandas as pd

# Load the existing GeoJSON file
geojson_file = "planning_area_2019.geojson"
gdf = gpd.read_file(geojson_file)

# Define your mapping as a dictionary
mapping_data = {
    "kml_1": "D16",
    "kml_2": "D22",
    "kml_3": "D21",
    "kml_4": "D03",
    "kml_5": "D23",
    "kml_6": "D10",
    "kml_8": "D17",
    "kml_9": "D23",
    "kml_10": "D05",
    "kml_11": "D19",
    "kml_12": "D22",
    "kml_13": "D22",
    "kml_14": "D18",
    "kml_15": "D22",
    "kml_16": "D19",
    "kml_17": "D05",
    "kml_18": "D28",
    "kml_19": "D27",
    "kml_20": "D19",
    "kml_21": "D19",
    "kml_22": "D12",
    "kml_23": "D24",
    "kml_25": "D12",
    "kml_26": "D27",
    "kml_27": "D02",
    "kml_28": "D25",
    "kml_29": "D12",
    "kml_30": "D22",
    "kml_32": "D24",
    "kml_33": "D25",
    "kml_34": "D09",
    "kml_35": "D08",
    "kml_36": "D06",
    "kml_37": "D01",
    "kml_39": "D15",
    "kml_40": "D01",
    "kml_41": "D15",
    "kml_42": "D02",
    "kml_43": "D07",
    "kml_44": "D08",
    "kml_45": "D09",
    "kml_46": "D02",
    "kml_47": "D16",
    "kml_48": "D10",
    "kml_49": "D23",
    "kml_50": "D26",
    "kml_51": "D20",
    "kml_52": "D20",
    "kml_53": "D14",
    "kml_54": "D18",
    "kml_55": "D26",
}

# Add a new column for district_code based on the mapping
gdf["district_code"] = gdf["Name"].map(mapping_data)

# Save the modified GeoJSON back to a file
gdf.to_file("planning_area_with_district.json", driver="GeoJSON")
