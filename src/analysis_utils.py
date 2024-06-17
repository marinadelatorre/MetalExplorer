import geopandas as gpd
import itertools
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import seaborn as sns

from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
from transformers import pipeline
from wordcloud import WordCloud


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


def calculate_centrality(df, group, target):
    G = nx.Graph()

    for band, group in df.groupby(group):
        targets = group[target].tolist()
        targets = [target.replace(' ', '\n') for target in targets]
        for target1, target2 in itertools.combinations(targets, 2):
            if target1!=target2 and not G.has_edge(target1, target2):
                G.add_edge(target1, target2)

    degree_centrality = nx.degree_centrality(G)
    betweenness_centrality = nx.betweenness_centrality(G)
    closeness_centrality = nx.closeness_centrality(G)
    eigenvector_centrality = nx.eigenvector_centrality(G)
    nodes_list = list(G.nodes())

    centrality_measures = {
        'Degree Centrality': degree_centrality,
        'Betweenness Centrality': betweenness_centrality,
        'Closeness Centrality': closeness_centrality,
        'Eigenvector Centrality': eigenvector_centrality
    }
    return G, centrality_measures


def create_centrality_graph(G, centrality_measure, label):
    norm = Normalize(vmin=min(centrality_measure.values()), vmax=max(centrality_measure.values()))
    node_colors = [plt.cm.autumn(norm(centrality_measure[node])) for node in G.nodes]

    plt.figure(figsize=(18, 14))
    pos = nx.spring_layout(G, seed=25, k=2.2)
    nodes = nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=4000, cmap=plt.cm.inferno)
    nx.draw_networkx_labels(G, pos, font_size=10)
    nx.draw_networkx_edges(G, pos, edge_color='gray', width=0.5)

    cax = plt.gca().inset_axes([1.05, 0.25, 0.03, 0.5])
    sm = ScalarMappable(cmap=plt.cm.inferno, norm=norm)
    sm.set_array([])
    plt.colorbar(sm, cax=cax, label=label)


    plt.title(f'Top 10 Genres by {label}')
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


def get_words_by_genre(df, column, genre):
    words = df.loc[df['Genre'] == genre][column].values.tolist()
    words = ' '.join(words)
    return words


def get_word_cloud(words):
    plt.figure(figsize=(8,6), facecolor='k')
    plt.imshow(WordCloud().generate(words))
    plt.axis("off")
    plt.show()
    

def get_sentiment(text, labels):
    pipe = pipeline(model="facebook/bart-large-mnli")
    result = pipe(text,
        candidate_labels=labels,
    )
    return result


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


def plot_frequency_map(df):
    cross_country = pd.crosstab(df['Country'], df['Genre'])
    most_frequent_genre = cross_country.idxmax(axis=1)
    country_genre_map = most_frequent_genre.to_dict()

    # Map genres to colors
    unique_genres = most_frequent_genre.unique()
    colors = plt.cm.get_cmap('tab20', len(unique_genres))
    genre_to_color = {genre.title(): colors(i) for i, genre in enumerate(unique_genres)}
    data_colored = {country: genre_to_color[genre.title()] for country, genre in country_genre_map.items()}

    # Load world map shapefile and add genre colors
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    world['color'] = world['name'].map(data_colored)
    world['color'] = world['color'].fillna('lightgray')

    # Plot the map
    fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    world.boundary.plot(ax=ax, color='gray', linewidth=0.5)
    world.plot(color=world['color'], ax=ax)

    # Create a custom legend and title
    legend_elements = [plt.Line2D([0], [0], marker='o', color='w', label=genre,
                                markersize=10, markerfacecolor=color) 
                    for genre, color in genre_to_color.items()]

    ax.legend(handles=legend_elements, loc='lower left', title='Genres', fontsize=10, title_fontsize=10)
    ax.set_title('Most Frequent Metal Genre in each Country', fontsize=20)
    ax.set_axis_off()

    plt.show()
