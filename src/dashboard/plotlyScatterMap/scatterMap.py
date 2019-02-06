import plotly
import plotly.plotly as py
import pandas as pd
from apiConfigs import plotlyConfigs
#plotly.tools.set_credentials_file(username=plotlyConfigs.plotly_username, api_key=plotlyConfigs.plotly_api_key)
mapbox_access_token = plotlyConfigs.mapbox_access_token

pdf.head()
df = pdf

data = [ dict(
        type = 'scattergeo',
        locationmode = 'USA-states',
        lon = pdf["lat"], # they seems to be flipped
        lat = pdf["long"], #
        text = df['tweet_text'],
        mode = 'markers',
        marker = dict(
            size = 8,
            opacity = 0.8,
            reversescale = True,
            autocolorscale = False,
            symbol = 'circle',
            line = dict(
                width=1,
                color='rgba(102, 102, 102)'
            ),
            )
        )]

layout = dict(
        title = 'Most trafficked US airports<br>(Hover for airport names)',
        colorbar = True,
        geo = dict(
            scope='usa',
            projection=dict( type='albers usa' ),
            showland = True,
            landcolor = "rgb(250, 250, 250)",
            subunitcolor = "rgb(217, 217, 217)",
            countrycolor = "rgb(217, 217, 217)",
            countrywidth = 0.5,
            subunitwidth = 0.5
        ),
    )

fig = dict( data=data, layout=layout )
py.plot( fig, validate=False, filename='d3-airports' )