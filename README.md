#New Column Creation

    def location330(a330_planning):

        df = a330_planning
        df = df[[df.msn, df.start_date, df.end_date, df.location, df.serie, df.today]]

        #For create a new column
        new_df = df.withColumn('position', df.location)

        return new_df

#Join of different dataframes

    def A330_location(geo_dataset, location330):
        from pyspark.sql.functions import col

        df_geo = geo_dataset
        df = location330

        df = df.join(df_geo, df.position == df_geo.location).select(df["*"], df_geo["lat"], df_geo["lon"])

        return df

#Replace column variable thorugh a table

    for row in moma:
        nationality = row[2]
        gender = row[5]

        nationality = nationality.replace("(","")
        nationality = nationality.replace(")","")

        gender = gender.replace("(","")
        gender = gender.replace(")","")

        row[2] = nationality
        row[5] = gender
