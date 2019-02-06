import plotly
import plotly.plotly as py
import pandas as pd
from apiConfigs import plotlyConfigs
#plotly.tools.set_credentials_file(username=plotlyConfigs.plotly_username, api_key=plotlyConfigs.plotly_api_key)
mapbox_access_token = plotlyConfigs.mapbox_access_token

df = result_pdf

# each class with the color it is given
scl = [[0, "rgb(5, 10, 172)"], [1, "rgb(40, 60, 190)"], [2, "rgb(70, 100, 245)"],\
    [3, "rgb(90, 120, 245)"], [4, "rgb(106, 137, 247)"]]

data = [dict(
        type='scattergeo',
        locationmode='USA-states',
        lon=df["lat"],  # they seems to be flipped
        lat=df["long"],  #
        text=df['tweet_text'],
        mode='markers',
        marker=dict(
            size=8,
            opacity=0.8,
            reversescale=True,
            autocolorscale=False,
            symbol='circle',
            line=dict(
                width=1,
                color='rgba(102, 102, 102)'
            ),
            colorscale=scl,
            cmin=0,
            color=df["prediction"],
            cmax=df["prediction"].max(),
            )
        )]

layout = dict(
        title='Tweet samples localized at california<br>(Hover for tweet text)',
        colorbar=True,
        geo=dict(
            scope='usa',
            projection=dict(type='albers usa'),
            showland=True,
            landcolor="rgb(250, 250, 250)",
            subunitcolor="rgb(217, 217, 217)",
            countrycolor="rgb(217, 217, 217)",
            countrywidth=0.5,
            subunitwidth=0.5
        ),
    )

fig = dict(data=data, layout=layout)
py.plot(fig, validate=False, filename='california_tweets')