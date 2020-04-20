# dataframe2html
Make the ability to show the image and the data of dataframe in notebook. 

## Usage

## How to use
- For Databricks notebook
- 1. Install Coordinates: io.github.fucusy:dataframe2html_2.11:0.1.3 from Maven to your cluster
- 2. Add `import io.github.fucusy.VizImplicit.VizDataFrame` to add toHTML function to DataFrame implicitly
- 3. Define your dataframe, `df`
- 4. Display the visualization, `displayHTML(df.toHTML())`

## How to publish to maven central

- Ask for credential from fucusy
- Update the version
- `sbt publishSigned`, it prepares the package in local
- `sbt sonatypeBundleRelease` to publish the project, will be synchronized to the Maven central within ten minutes.
- Commit the new version to master if the publish is successful, because the we won't able to publish the existing version anymore
