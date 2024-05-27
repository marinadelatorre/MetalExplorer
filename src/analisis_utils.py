import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd

def plot_activity_years(df):
    df.reset_index(drop=True, inplace=True)

    min_year = df['Start'].min()
    max_year = df['End'].max()
    num_years = max_year - min_year + 1

    # Create a matrix to store the activity years of bands
    activity_matrix = np.zeros((len(df), num_years))

    # Fill the activity matrix
    for i, row in df.iterrows():
        start_idx = row['Start'] - min_year
        end_idx = row['End'] - min_year
        activity_matrix[i, start_idx:end_idx+1] = 1

    plt.xlabel('Year')
    plt.ylabel('Number of Bands')
    plt.title('Activity Years of Bands')
    plt.imshow(activity_matrix, cmap='Oranges', aspect='auto', extent=[min_year, max_year+1, len(df), 0], vmin=0, vmax=1)
    plt.grid(axis='x')

    plt.show()


def create_crosstable_heatmap(df, x, y, n_x=20, n_y=10):
    # top_x = df[x].value_counts().nlargest(n_x).index.tolist()
    top_y = df[y].value_counts().nlargest(n_y).index.tolist()
    df = df[df[y].isin(top_y)]
    df.loc[:, y] = df[y].str.title() if df[y].dtype == 'object' else df[y]

    cross_table = pd.crosstab(df[x], df[y])
    cross_table['Total'] = cross_table.sum(axis=1)
    cross_table.sort_values(by='Total', ascending=False, inplace=True)

    plt.figure(figsize=(12, 8))
    cross_table = cross_table.iloc[:n_x, :]
    sns.heatmap(cross_table.drop(columns=['Total']), annot=True, cmap="hot", linewidths=.5, fmt='d')

    plt.title(f"Number of Bands in {y} per {x}")
    plt.xlabel(y)
    plt.ylabel(x)
    plt.show()
    
    return cross_table


def plot_choropleth_map(df, entity):
    country_counts = df['Country'].value_counts().reset_index()
    country_counts.columns = ['country', f'{entity}_count']

    bins = [1, 10, 50, 100, 300, 500, 1000, 2000]
    labels = ['1-10', '10-50', '50-100', '100-300', '300-500', '500-1000', '1000+']
    country_counts[f'{entity}_count_binned'] = pd.cut(country_counts[f'{entity}_count'], bins=bins, labels=labels, right=False)

    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    world = world.merge(country_counts, how='left', left_on='name', right_on='country')

    fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    world.boundary.plot(ax=ax, color='gray', linewidth=0.5)
    world.plot(column=f'{entity}_count_binned', ax=ax, legend=True,
            legend_kwds={'title': f"Number of {entity}s"},
            cmap='Reds', missing_kwds={
                "color": "lightgrey",
                "edgecolor": "gray",
                "hatch": "///",
                "label": "No data"
            })

    ax.set_title(f"Number of {entity}s by Country", fontdict={'fontsize': 20})
    ax.set_axis_off()

    fig.subplots_adjust(left=0.1)
    legend = ax.get_legend()
    legend.set_bbox_to_anchor((0.25, 0.55))
    legend.set_title(f"Number of {entity}s")
    legend._legend_box.align = "left"

    plt.show()