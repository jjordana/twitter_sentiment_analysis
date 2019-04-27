#For opening any CSV file in python
    
    from csv import reader

    # Read the `artworks_clean.csv` file
    opened_file = open('artworks_clean.csv')
    read_file = reader(opened_file)
    moma = list(read_file)
    moma = moma[1:]

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

#If I want to clean the values of a column, removing certain characters, I can use:

    test_data = ["1912", "1929", "1913-1923",
                 "(1951)", "1994", "1934",
                 "c. 1915", "1995", "c. 1912",
                 "(1988)", "2002", "1957-1959",
                 "c. 1955.", "c. 1970's", 
                 "C. 1990-1999"]

    bad_chars = ["(",")","c","C",".","s","'", " "]

    def strip_characters(string):
        for char in bad_chars:
            string = string.replace(char, "")
        return string

    stripped_test_data = []

    for s in test_data:
        s = strip_characters(s)
        stripped_test_data.append(s)
