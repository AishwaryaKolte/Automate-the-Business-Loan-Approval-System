#!/usr/bin/env python
# coding: utf-8

# # Automating Business Loan Approval Using SQL

# ![Precentage%20of%20default%20and%20paid%20loans.png](attachment:Precentage%20of%20default%20and%20paid%20loans.png)
# 
# 
#   ###                Time Series of Default Rates till 2020

# ![Timeseries%20default%20rates.png](attachment:Timeseries%20default%20rates.png)

# ### Top 10 Customers
# 
# 
# ![top10%20customers.png](attachment:top10%20customers.png)

# ### Glimpse of Defualt rates by the top customers 
# 
# 
# 
# ![series%20ofloans%20and%20defaults%20by%20top%20customers%20.png](attachment:series%20ofloans%20and%20defaults%20by%20top%20customers%20.png)

# * Loans are given against Real Estate Property if the term of the loan is 20 years or more that is 240 months
#   We have created a new column that would indicate if a loan was taken against industry property
#   
# * Additionally the lst two digits of NACIS indicate the industry secort we have extarcted the same from the column
# 
# `Note : Some of the DDL and DML commands used have been mentioned in the selected metrics pdf`
# 
# `Additionally more of these commands was used `
# 
# In this file we will only focus on creating a query that will help to focus out likely to default loans and best loan applications
# 
# Additionally this is just an human assumption with no focus on accuracy 

# In[5]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# In[6]:


import pandas.io.sql as sqlio
import psycopg2 as ps


# In[10]:


conn=ps.connect(dbname="mentormind",
                user="postgres",
                password="6282076096SQL",
                host="localhost",
                port="5432"
)


# In[4]:


sql="""select * from sbanational """


# In[5]:


df=sqlio.read_sql_query(sql,conn)
df


# In[6]:


print(df.columns)
default_filter=df['MIS_Status']=="CHGOFF"
default_df=df[default_filter]
paid_df=df[~default_filter]

pd.set_option('display.float_format', lambda x: '%.3f' %x)
default_df['DisbursementGross'].describe()


# In[7]:


pd.set_option('display.float_format', lambda x: '%.3f' %x)
paid_quartiles=paid_df['DisbursementGross'].describe().to_frame().reset_index().rename(columns={'DisbursementGross':"paid"})
paid_quartiles

default_quartiles=default_df['DisbursementGross'].describe().to_frame().reset_index().rename(columns={'DisbursementGross':"Default"})
default_quartiles

quartiles=pd.merge(paid_quartiles,default_quartiles,on="index")
quartiles


# # Industry Analysis

# In[8]:


industry_deaults_query="""select "industry_sector",
avg(case when "MIS_Status"='CHGOFF' then 1 else 0 end) as default_rate
from sbanational
group by 1"""
industry_defaults_df=sqlio.read_sql_query(industry_deaults_query,conn)
industry_defaults_df.sort_values(by="default_rate",inplace=True)


# In[9]:


industry_defaults_df.head()


# In[10]:


import matplotlib.pyplot as plt

plt.figure(figsize=(12, 6))  # Adjust figure size for better visualization

# Create the bar plot with custom color and edge color
bars = plt.bar(industry_defaults_df['industry_sector'],
               industry_defaults_df['default_rate'],
               color='skyblue', edgecolor='black')

plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.xlabel('Industry Sectors')  # X-axis label
plt.ylabel('Default Rate')  # Y-axis label
plt.title('Default Rate by Industry Sector')  # Title of the plot

# Add data labels to the top of each bar
for bar in bars:
    plt.text(bar.get_x() + bar.get_width() / 2 - 0.1,  # Adjust text position
             bar.get_height() + 0.005,  # Adjust height
             f'{bar.get_height():.2%}',  # Display percentage with two decimal places
             ha='center', color='black', fontsize=9)

plt.tight_layout()  # Adjust layout for better fit
plt.show()


# In[11]:


industry_defaults_cohort_query="""

select bb.industry_sector,
date_part('year',age(bb."ApprovalDate",aa.first_year)) as periods,
date_part('year',bb."ApprovalDate") as current_year,
count(distinct bb."LoanNr_ChkDgt ") as total_loans ,
AVG(case when "MIS_Status"='CHGOFF' THEN 1 ELSE 0 END) AS default_rate
from
(
select "industry_sector",min("ApprovalDate") as first_year
from sbanational
group by 1
)aa
join sbanational bb on aa."industry_sector"=bb."industry_sector" 
group by 1,2,3
"""
industry_cohort=sqlio.read_sql_query(industry_defaults_cohort_query,conn)


# In[12]:


industry_cohort.head()


# In[13]:


industry_cohort.info()


# In[16]:


df.columns


# In[17]:


paid_quartiles=paid_df['SBA_Appv'].describe().to_frame().reset_index().rename(columns={'SBA_Appv':"paid"})
paid_quartiles

default_quartiles=default_df['SBA_Appv'].describe().to_frame().reset_index().rename(columns={'SBA_Appv':"Default"})
default_quartiles

newjob_quartiles=pd.merge(paid_quartiles,default_quartiles,on="index")
newjob_quartiles


# In[18]:


bank_query="""
select "BankState",
avg(case when "MIS_Status"='CHGOFF' THEN 1 ELSE 0 END) as default_rate,COUNT(1)
FROM SBANATIONAL
GROUP BY 1
having COUNT(1) > 10
ORDER BY 2 DESC
"""
bank_df=sqlio.read_sql_query(bank_query,conn)
bank_df.head()


# In[27]:


#Top 6 States with highest Default rates
import seaborn as sn



plt.subplot(2,1,1)
plt.title("Top 6  States with highest default_Rate")
sn.barplot(data=bank_df.head(6),x="BankState",y="default_rate",hue="BankState")
plt.show()


plt.subplot(2,1,2)
plt.title("Top 6  States with highest number of loans")
sn.barplot(data=bank_df.sort_values("count",ascending=False).head(6),x="BankState",y="count",hue="BankState")
plt.show()


# # Insights :
#  All loans from the state VA can be excluded because it has significantly more default rate with very low number of loans :38% defaults
#  
#  NC also has 28% defaults but in this case we will focus on the state NC and see if there is any industry in particular that is causing
#  this much defaults

# In[42]:


#State NC industries
#and cross check the inights with a few more states at random("OH","CA","SD","IL","TX")
states_query="""

select "BankState","industry_sector",
avg(case when "MIS_Status"='CHGOFF' THEN 1 ELSE 0 END) as default_rate
,COUNT(1)
FROM SBANATIONAL
where "BankState" in ('NC','OH','CA','SD','IL','TX')
GROUP BY 1,2
having COUNT(1) > 10
ORDER BY 3 DESC

"""
states_filter_df=sqlio.read_sql_query(states_query,conn)
states_name=states_filter_df['BankState'].unique()
print(states_name)
states_filter_df


# In[21]:


#Use some filters
#Exclude staes with higher default rate
#Accept applicateions with higher term 
#When loooking at top customers they also have default rates some with higher defaults at the begining loans
#later becomming lower and some with lower gradually increasing 
#Discard states with lower number of loans and which has higher default rates


# In[28]:


bank_df.columns


# In[52]:


bank_df.sort_values(by=["count","default_rate"],ascending=[1,0]).head()


# In[56]:


states_filter_df.info()


# In[60]:


pivot_df = states_filter_df.pivot_table(values='default_rate', columns='BankState', index='industry_sector')
pd.DataFrame(pivot_df)


# In[65]:


sn.heatmap(pivot_df, annot=True, cmap='YlGnBu', fmt='.2f')


# In[ ]:


#There is no clear indicator of a indutry of defaulting 
#But among the different industries Real Estate and Leasing has considerabbly higher default rate among peer states
#Second will be Finance and insurance


# In[66]:


banks_query="""
with cte as(
select distinct "Bank",count(1),
avg(case when "MIS_Status"='CHGOFF'then 1 else 0 end) as default_rate
from sbanational
group by 1
having count(1) > 10
order by 3 desc
)
select * from cte
where default_rate >0.5
"""
banks_df=sqlio.read_sql_query(banks_query,conn)

banks_df.head()


# In[ ]:


list_of_defaulter_banks=list(banks_df['Bank'][0:len(banks_df)-1])
list_of_defaulter_banks
#We will discard all the loan applications from this bank


# In[21]:


#UPDATE your_table
#SET column_name = REPLACE(column_name, '"', '');

#The above code is used to remove "" from all fields in the industry sector column
#This Column was derived from NAICS column where last two digit represt the industry sector

industry_cohort_query="""
select
bbb.industry_sector,
date_part('year',bbb."ApprovalDate") as years,
avg(case when bbb."MIS_Status"='CHGOFF' then 1 else 0 end) as default_rate
,count(distinct "LoanNr_ChkDgt ") as number_of_loans
from
(select "industry_sector",min("ApprovalDate") as first_date
from sbanational
group by 1) as aaa
join sbanational as bbb
on aaa."industry_sector"=bbb."industry_sector"
group by 1,2
order by 1,2
"""
industry_cohort_df=sqlio.read_sql_query(industry_cohort_query,conn)
industry_cohort_df


# In[46]:


industry_names=industry_cohort_df['industry_sector'].unique()
industry_names


# In[44]:


def line_bar(industry):
    plt.figure(figsize=(14, 6))
    plot_df=industry_cohort_df[industry_cohort_df['industry_sector']==industry]

    plt.subplot(1,2,1)
    plt.title(f"Defualt rate in {industry}")
    sn.lineplot(data=plot_df,x="years",y="default_rate")
    
    
    plt.subplot(1,2,2)
    sn.barplot(data=plot_df,x="years", y="number_of_loans", palette='plasma')

    # Adding labels to the bars (Horizontal bar plot)
    for index, value in enumerate(plot_df["number_of_loans"]):
        plt.text(index, value, str(value), ha='center', va='bottom')

    plt.xlabel('Categories')
    plt.ylabel('Values')
    plt.xticks(rotation=60)
    plt.title(f'Number of loans by {industry}')
    plt.tight_layout()
    plt.show()

    # Adding labels to the bars
    #for index, value in enumerate(values):
      #  plt.text(value, index, str(value))
    plt.tight_layout()
    plt.show()


# In[45]:


for i in industry_names:
    line_bar(i)
    


# # Decsion Criteria
# Defaulter
# 
# * States:PR,VC
# * Specific banks
# * Gross Disbursment less than 43,62,157
# * Term less than 240 months or 20 years
# * Industries include  Real Estate and Leasing & Finance and insurance
# 
# 
# These indicators will likely decrease the default rates but these are not solid indicators 
# If we were to build any classification Model for classifing defaulter loans we would have considerably more siginifiacant accuracy in predicting loan applicatios that are likely to default

# In[75]:





# In[ ]:


#Question :Does the decision to be taken for the first loan of a customers 
#         :OR does the decision to be taken is then cumilative count of applications where we arent seeing any sort of
#         :progress in the decreasing of default rate


# ### Adding New column named Loan_Eligibility
# 
# `ALTER TABLE sbanational`
# 
# `ADD column "Loan_Eligibility" VARCHAR(100)`
# 
# 

# update sbanational
# set Loan_Eligibility=  query

# In[47]:


final_query="""
select 
---Higher Disbursement Gross indicate companys asset power to Pay back
case when "DisbursementGross" >= 4360000 then 1
---Avoiding these two Bank States cause of higher defaults
when "BankState" not in ('PR','VC') then 1 
---Including Loans against property
when real_estate=1 then 1
---Excluding Risky Industry Sectors
when industry_sector not in ('Finance and insurance','Real estate and rental and leasing')
then 1
---Selecting Banks with consistent defaults and high rate of default
when "Bank" not in ('FDIC/ALLIANCE BANK',
 'FDIC/FIRST INTERSTATE BANK',
 'FDIC/MONCOR BANK',
 'FDIC/WESTERN BANK',
 'FIRST COMMERCIAL MORTGAGE',
 'FDIC/ALASKA CONTINENTAL BANK',
 'UNITED WESTERN BANK',
 'CAPITAL ONE BK (USA) NATL ASSO',
 'SHOREBANK',
 'FIRSTCITY BANK',
 'SECURITY NATIONAL PARTNERS',
 'M&I, A BRANCH OF',
 'SOUTHLAND CREDIT UNION',
 'UTAH FIRST FCU',
 'SUPERIOR FINANCIAL GROUP, LLC',
 'LOGIX FCU',
 'THE LEGACY BANK, A DIVISION OF',
 'UNITED SAN ANTONIO COMMUN FCU',
 'CERTIFIED FCU',
 'FDIC/MECHANICS BANK',
 'COMM. SERVICES OF PERRY INC',
 'HARRIS N.A., A BRANCH OF',
 'FEDERAL DEPOSIT INSUR CORP',
 'DMB COMMUNITY BANK',
 'BENEFICIAL STATE BANK',
 'BUS. DEVEL CORP - GEORGIA I',
 'WAYNE COUNTY BANK',
 'BBCN BANK',
 'HORIZON UTAH FCU D/B/A HORIZON',
 'MONARCH COMMUNITY BANK',
 'BLOOMFIELD STATE BANK',
 'RBC BANK (USA)',
 'BEEHIVE CU',
 'RANDOLPH-BROOKS FCU',
 'SAFE CU') 
then 1

else 0
end
as approved
from sbanational

"""


# In[48]:


# resulting_df=sqlio.read_sql_query(final_query,conn)
#Note Running the finallquery marks only 11 from 899153 as chances of default

