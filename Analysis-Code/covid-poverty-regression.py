import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def cor_matrix(x, f_name):
    correlation_matrix = x.corr().round(2)
    sns.heatmap(correlation_matrix, cmap='YlGnBu')
    plt.tight_layout()
    plt.plot()
    plt.savefig(f_name)
    plt.close()


def reg(data, x, y, f_name):
    sns.regplot(data=data, x=x, y=y)
    plt.plot()
    plt.savefig(f_name)
    plt.close()


# COVID and Employment
cvd_emp = pd.read_csv('cvd_employment_regression_data.csv')
name = 'covid_employment_correlation_matrix.png'

cvd_emp.drop(columns=['date',
                      'trade_transportation_utilites', 'pub_admin', 'wholesale',
                      'manufacturing', 'information'], inplace=True)
cvd_emp.rename(columns={'total_nonfarm': 'Total Nonfarm', 'total_private': 'Total Private',
                        'education_and_health_svcs': 'Education & Health Svcs.',
                        'professional_business_svcs': 'Professional Business Svcs.',
                        'finance': 'Finance', 'leisure_hospitality': 'Leisure & Hospitality',
                        'retail': 'Retail', 'cases': 'Cumulative Cases',
                        'hospitalizations': 'Cumulative hospitalizations',
                        'deaths': 'Cumulative deaths'}, inplace=True)
cor_matrix(cvd_emp, name)

files = [('covid_total_nonfarm_regression.png', 'Total Nonfarm'),
         ('covid_edhealth_regression.png', 'Education & Health Svcs.'),
         ('covid_profession_bus_regression.png', 'Professional Business Svcs.'),
         ('covid_leisure_hospitality_regression.png', 'Leisure & Hospitality'),
         ('covid_retail_regression.png', 'Retail')]
for file in files:
    reg(cvd_emp, 'Cumulative Cases', file[1], file[0])

# COVID and Homelessness
cvd_dhs = pd.read_csv('cvd_DHS_regression_data.csv')
name = 'covid_homelessness_correlation_matrix.png'

cvd_dhs.rename(columns={'avg_Adults_in_Shelter': 'Adults in Shelter',
                        'avg_Children_in_Shelter': 'Children in Shelter',
                        'cumulative_cases': 'Cumulative Cases',
                        'cumulative_hospitalizations': 'Cumulative hospitalizations',
                        'cumulative_deaths': 'Cumulative deaths'}, inplace=True)

cvd_dhs.drop(columns=['date', 'cases', 'hospitalizations', 'deaths', 'deaths_prob'], inplace=True)
cor_matrix(cvd_dhs, name)

files = [('covid_adult_homelessness_regression.png', 'Adults in Shelter'),
         ('covid_child_homelessness_regression.png', 'Children in Shelter')]
for file in files:
    reg(cvd_dhs, 'Cumulative Cases', file[1], file[0])
