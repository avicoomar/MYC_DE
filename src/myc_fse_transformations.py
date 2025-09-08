def transform(dataframes):
	auth_group = dataframes["auth_group"]
	auth_group_permissions = dataframes["auth_group_permissions"]
	auth_permission = dataframes["auth_permission"]
	users_company = dataframes["users_company"]
	users_entrepreneurdetail = dataframes["users_entrepreneurdetail"]
	users_investordetail = dataframes["users_investordetail"]
	users_myuser = dataframes["users_myuser"]
	users_myuser_groups = dataframes["users_myuser_groups"]
	users_myuser_user_permissions = dataframes["users_myuser_user_permissions"]
	
	# Renaming the columns to avoid confusion
	users_myuser = users_myuser.withColumnRenamed("id", "user_id")
	users_investordetail =  users_investordetail.withColumnRenamed("id", "investor_id")
	users_entrepreneurdetail = users_entrepreneurdetail.withColumnRenamed("id", "entrepreneur_id") 
	users_company = users_company.withColumnRenamed("id", "company_id")
	
	# Joining users_entrepreneurdetail & users_company into one
	user_ent_comp = users_entrepreneurdetail.join(users_company, "entrepreneur_id", "full_outer")
	
	# Create user_facts: 
	user_facts = users_myuser.join(users_investordetail, "user_id", "full_outer").join(user_ent_comp, "user_id", "full_outer")
	user_facts = user_facts.select("user_id", "investor_id", "entrepreneur_id")
	
	# Dropping user_id columns as that can be fetched from fact table:
	user_ent_comp = user_ent_comp.drop("user_id")
	users_investordetail = users_investordetail.drop("user_id")
	
	# Dropping content_type_id as it is irrelevant:
	auth_permission = auth_permission.drop("content_type_id")
	
	# Renaming the columns to avoid confusion & for joining purpose:
	auth_group_permissions = auth_group_permissions.withColumnRenamed("permission_id", "group_permission_id")
	users_myuser_user_permissions = users_myuser_user_permissions.withColumnRenamed("permission_id", "user_permission_id")
	users_myuser_user_permissions = users_myuser_user_permissions.withColumnRenamed("myuser_id", "user_id")
	users_myuser_groups = users_myuser_groups.withColumnRenamed("myuser_id", "user_id")

	user_permission_facts = users_myuser_groups \
		.join(auth_group_permissions, "group_id", "full_outer") \
		.join(users_myuser_user_permissions, "user_id", "full_outer") \
		.select("user_id", "user_permission_id", "group_id", "group_permission_id")
	
	transformed_dataframes = {
		"users_myuser": users_myuser,
		"users_investordetail": users_investordetail,
		"user_ent_comp": user_ent_comp,
		"user_facts": user_facts,
		"auth_group": auth_group,
		"auth_permission": auth_permission,
		"user_permission_facts": user_permission_facts,
	}
	
	return transformed_dataframes
