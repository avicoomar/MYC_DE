from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, split, regexp_replace, regexp_extract, size, when, explode, btrim, lit, udf

@udf
def extract_facebook_link(links):
	for link in links:
	    if "facebook" in link:
	        return link
@udf
def extract_twitter_link(links):
	for link in links:
	    if "twitter" in link:
	        return link
@udf
def extract_linkedin_link(links):
	for link in links:
	    if "linkedin" in link:
	        return link
@udf
def extract_website_link(links):
	for link in links:
	    if "facebook" not in link and "twitter" not in link and "linkedin" not in link :
	        return link

def transform_dataframes(df):
	
	# Adding "id" column to dataframe
	temp_df = df.rdd.zipWithIndex().toDF()
	df = temp_df.select(col("_2").alias("id"), col("_1.*"))
	
	# Removing spaces and "\n" from meta column
	df = df.withColumn("meta", regexp_replace(df.meta, r"(\s{2,})|\\n", ""))
	# Splitting meta column into sub-categories
	df = df.withColumns({
		"type": trim(split(df.meta, "·").getItem(0)),
		"location": trim(split(df.meta, "·").getItem(1)),
		"investments_last_year": when(size(split(df.meta, "·"))>=3, trim(split(df.meta, "·").getItem(2))).otherwise(None),
	})
	# Extracting only number of investments in investments_last year
	df = df.withColumn("investments_last_year", regexp_extract(df.investments_last_year, r"(.*(?=(\sinvestments)))", 1))
	
	# Splitting links column into sub-categories
	df = df.withColumns({
		"linkedin": extract_linkedin_link(df.links),
		"twitter": extract_twitter_link(df.links),
		"facebook": extract_facebook_link(df.links),
		"website": extract_website_link(df.links)
	})
	
	# Removing spaces and "\n" from desc column
	df = df.withColumn("desc", regexp_replace(df.desc, r"(\s{2,})|\\n", ""))
	
	# Splitting investment_focus into sub-categories
	df = df.withColumns({
		"sector": df.focus.getItem(0),
		"stage": df.focus.getItem(1),
		"region": df.focus.getItem(2),
	})
	
	# Flattening each value of sector, stage, region into multiple rows
	df = df.withColumn("sector", split(df.sector, ","))
	df = df.withColumn("sector", explode(df.sector))
	df = df.withColumn("stage", split(df.stage, ","))
	df = df.withColumn("stage", explode(df.stage))
	df = df.withColumn("region", split(df.region, ","))
	df = df.withColumn("region", explode(df.region))

	# Flatten each value of portfolio_highlights into multiple rows
	df = df.withColumn("portfolio_highlights", explode(df.portfolio_highlights))
	
	# Splitting portfolio_highlights into sub-categories
	df = df.withColumns({
		"portfolio_name": btrim(split(df.portfolio_highlights, "—").getItem(1), trim=lit(" \n")),
		"portfolio_link": trim(split(df.portfolio_highlights, "—").getItem(0)),
		"portfolio_desc": btrim(split(df.portfolio_highlights, "—").getItem(2), trim=lit(" \n"))
	})
	
	# Final tables:
	df_desc = df.select(col("id").alias("investor_id"), "desc").distinct().sort("investor_id")
	df_portfolio_desc = df.select(col("id").alias("investor_id"), "portfolio_desc").distinct().sort("investor_id")
	df = df.select(
		"id",
		"name",
		"type",
		"location",
		"investments_last_year",
		"sector",
		"stage",
		"region",
		"portfolio_name",
		"portfolio_link",
		"linkedin",
		"facebook",
		"twitter",
		"website",
	)
	
	return {
		"df": df,
		"df_desc": df_desc,
		"df_portfolio_desc": df_portfolio_desc
	}
