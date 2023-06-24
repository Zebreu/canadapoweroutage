import numpy as np
import datetime
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from geopy.geocoders import Nominatim
import psycopg2
import folium
import folium.features
from streamlit_folium import st_folium
import os

#from pyproj import Geod
#geod = Geod(ellps="WGS84")

st.set_page_config(page_title='Historical power outage in Canada', layout="wide")

time_zone_adjustment = datetime.timedelta(hours=4) # EST

psqlcreds = os.getenv('psqlcreds')

@st.cache_resource
def create_connection(url):
    conn = psycopg2.connect(psqlcreds)
    cursor = conn.cursor()
    return cursor


@st.cache_data(ttl=1800)
def get_timestamps():
    cursor = create_connection('same')

    cursor.execute('''SELECT distinct timestamp from twkboutagepolygons 
                    order by timestamp;''')
    timestamps = cursor.fetchall()
    timestamps = [t[0] for t in timestamps]
    return timestamps


@st.cache_data(ttl=1800)
def get_total_area():
    cursor = create_connection('same')
    cursor.execute('select timestamp, sum(st_area(geom)) from outagepolygon_view group by timestamp')
    areas = cursor.fetchall()
    areas = pd.DataFrame(areas, columns=['Time','Area of Quebec impacted'])
    return areas


def get_duration(frame):
    duration = max(frame['Time']) - min(frame['Time'])
    if duration.total_seconds() < 15:
        duration = datetime.timedelta(minutes=10)

    return duration.total_seconds()

def calculate_size_for_zoom(zoom):
    zoom = max(9, zoom)
    meters_per_pixel = 9.555*(2**(14-zoom))
    radius = meters_per_pixel*120
    precision = (0.001/9.555)*meters_per_pixel
    return radius, precision

@st.cache_data(ttl=1800)
def map_neighborhood(_cursor, center = (45.446892,-75.790369), last_outage_time = None, zoom_level = 15):
    timestamps = get_timestamps()
    timestamps = [t-time_zone_adjustment for t in timestamps]

    if last_outage_time is None:
        last_outage_time = timestamps[-1]
    #requested_time = st.select_slider("Date",options=timestamps,value=last_outage_time)

    radius, precision = calculate_size_for_zoom(zoom_level)

    if zoom_level < 12:
        query = '''WITH grid AS (
            select 
                (ST_HexagonGrid(%s, st_buffer(st_setsrid(st_point(%s,%s),4326)::geography,%s)::geometry)).*
            ),
            itemgrid as (
            select row_number() over () as item, geom from grid
            ),
            heatmap as (
                select itemgrid.item as item, count(distinct outagepolygon_view.timestamp) as outage, itemgrid.geom as geom
                from outagepolygon_view
                inner join itemgrid 
                on outagepolygon_view.geom && itemgrid.geom group by itemgrid.item, itemgrid.geom
            )
            select json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(ST_asgeojson(heatmap.*)::json)
                ) from heatmap;'''    
    else:
        query = '''WITH grid AS (
            select 
                (ST_HexagonGrid(%s, st_buffer(st_setsrid(st_point(%s,%s),4326)::geography,%s)::geometry)).*
            ),
            itemgrid as (
            select row_number() over () as item, geom from grid
            ),
            heatmap as (
                select itemgrid.item as item, count(distinct outagepolygon_view.timestamp) as outage, itemgrid.geom as geom
                from outagepolygon_view
                inner join itemgrid 
                on st_intersects(outagepolygon_view.geom, itemgrid.geom) group by itemgrid.item, itemgrid.geom
            )
            select json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(ST_asgeojson(heatmap.*)::json)
                ) from heatmap;'''
        
    _cursor.execute(query, (precision, center[1], center[0], radius))
    
    _cursor.connection.commit()    
    polygons = _cursor.fetchall()[0][0]
    
    whatever = pd.DataFrame()
    whatever['outage'] = [p['properties']['outage'] for p in polygons['features']]
    whatever['item'] = [p['properties']['item'] for p in polygons['features']]
    maximum_outages =  max(whatever['outage'])
    fig = px.choropleth_mapbox(whatever, geojson=polygons, locations='item', 
                        color='outage',
                        featureidkey="properties.item",
                        color_continuous_scale="Viridis",
                        range_color=(0, maximum_outages),
                        mapbox_style="carto-positron",
                        zoom=zoom_level, center = {"lat": center[0], "lon": center[1]},
                        opacity=0.5,
                        labels={'outage':'Number of outage periods'}
                        )
    
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=900)
    #line_colors = px.colors.sample_colorscale('Viridis', whatever['outage'].values/maximum_outages)
    fig.update_traces(marker_line_color='white')
    
    st.plotly_chart(fig, use_container_width=True)    

def map_it(cursor, center = (45.446892,-75.790369), last_outage_time = None):
    timestamps = get_timestamps()
    timestamps = [t-time_zone_adjustment for t in timestamps]

    if last_outage_time is None:
        last_outage_time = timestamps[-1]

    requested_time = st.select_slider("Date",
                                    options=timestamps,
                                      value=last_outage_time)
    query = '''select json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(ST_asgeojson(outagepolygon_viewmore.*)::json)
                )
                from (select ROW_NUMBER() over (),
                geom from outagepolygon_view where timestamp = %s)
                as outagepolygon_viewmore(item, geom);'''

    
    cursor.execute(query, (requested_time+time_zone_adjustment,))
    cursor.connection.commit()    
    polygons = cursor.fetchall()[0][0]
    
    whatever = pd.DataFrame()
    whatever['outage'] = [2]*len(polygons['features'])
    whatever['item'] = range(1, len(polygons['features'])+1)

    fig = px.choropleth_mapbox(whatever, geojson=polygons, locations='item', 
                        color='outage',
                        featureidkey="properties.item",
                        color_continuous_scale="Viridis",
                        range_color=(0, 12),
                        mapbox_style="carto-positron",
                        zoom=15, center = {"lat": center[0], "lon": center[1]},
                        opacity=0.5,
                        labels={'outage':'Outage location'}
                        )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=900)

    st.plotly_chart(fig, use_container_width=True)


@st.cache_data(ttl=1800)
def get_outage_polygons(_cursor, x, y):
    query = '''select timestamp from outagepolygon_view 
                where ST_Intersects(ST_SetSRID(ST_Point(%s, %s), 4326), 
                outagepolygon_view.geom)'''
    
    _cursor.execute(query, (x, y))
    hits = _cursor.fetchall()
    return hits


@st.cache_data(ttl=3600)
def get_point(modified_address):
    try: 
        geolocator = Nominatim(user_agent="quebecpowerdashboard")
        address = geolocator.geocode(modified_address)
        st.write(f'{address.address}, {address.latitude}, {address.longitude}')
        point = (address.longitude, address.latitude)
    except:
        st.write("Sorry, we can't find you, please fill out the next form")
        location = st.text_input('Write latitude and longitude separated by a comma', '45.3932, -75.8236')
        lat, lon = location.split(',')
        point = (float(lon),float(lat))
    
    return point[0], point[1]

@st.cache_data(ttl=3600)
def draw_timeline(hits):
    areas = get_total_area()
    areas['Time'] = areas['Time'] - time_zone_adjustment

    requested_results = pd.DataFrame(hits, columns=['Time'])
    requested_results['Time'] = requested_results['Time'] - time_zone_adjustment
    
    maximum_area = max(areas['Area of Quebec impacted'])
    requested_results['Outage at address'] = maximum_area
    
    results = pd.merge(left=areas, right=requested_results, how='left', left_on='Time', right_on='Time')
    #results.index = results['Time']
    #resampled = results.resample('120T').sum()

    new_frames = []
    groups = results['Outage at address'].isna().cumsum()
    for _, group in results.groupby(groups):
        new_frame = group.dropna()
        if len(new_frame) > 0:
            new_frames.append(new_frame)

    durations = [get_duration(frame) for frame in new_frames]

    fig = px.line(areas, x='Time', y='Area of Quebec impacted', 
                    title="Outages at the selected address against general outages in Quebec")        
    
    
    for outage_number, new_frame in enumerate(new_frames):
        markersize = 1
        if durations[outage_number] < 20*60:
            markersize = 8
        fig.add_trace(go.Scatter(x=new_frame['Time'], 
                                y=new_frame['Outage at address'],
                    marker=dict(size=markersize), 
                    name=f"Outage {outage_number}",
                    fill='tozeroy',
                    fillcolor='purple',
                    hoveron = 'points+fills',
                    line_color='purple',
                    text="Outage at address",
                    hoverinfo = 'text+x+y'))
    #fig.update_xaxes(rangeslider_visible=True)

    st.plotly_chart(fig, use_container_width=True)

    st.write(f'{len(new_frames)} outages since April 2023, with an median duration of {int(np.median(durations)/60)} minutes')
    for frame_number, frame in enumerate(new_frames):
        st.write(f"Outage {frame_number} happened at {frame['Time'].iloc[0]} and lasted about {int(durations[frame_number]/60)} minutes")
    return new_frames

def selection_map(center):
    m = folium.Map(location=center, zoom_start=15)
    st.write('Select a frame to visualize using the map on the left then click the button')

    folium_output = st_folium(
        m,
        center=st.session_state["center"],
        zoom=st.session_state["zoom"],
        key="new",
        height=500,
        width=500,
        returned_objects=['zoom', 'center']
    )

    heatmap_clicked = st.button('Click here to see the outage heatmap')
    return folium_output, heatmap_clicked

def main():
    cursor = create_connection('same')
    cursor.connection.commit()

    raw_address = st.text_input('Address:', "41 Avenue Saint-Just, Montreal")
    modified_address = raw_address + ', Quebec, Canada'
    (x,y) = get_point(modified_address)
    
    #round_timestamp = pd.Timestamp.now().round('30min').timestamp()
    hits = get_outage_polygons(cursor, x, y)
     
    if hits:
        new_frames = draw_timeline(hits)
    else:
        st.write('Looking good, no outage for you!')

    if "center" not in st.session_state:
        st.session_state["center"] = [y,x]
    if "zoom" not in st.session_state:
        st.session_state["zoom"] = 15
    
    original_center = (y,x)
    
    cumulative_map = st.checkbox('Show cumulative heatmap')
    if cumulative_map:
        heatmap_center = st.session_state['center']
        col1, col2 = st.columns([0.3,0.7])
        with col1:
            folium_output, heatmap_clicked = selection_map(center=original_center)
            st.write('Be aware that the heatmap may take a minute to load for large urban areas')
            if heatmap_clicked:
                if folium_output['zoom'] < 9:
                    st.write('Maximum zoom level reached. ')
                
        with col2:
            if heatmap_clicked:
                st.session_state['zoom'] = folium_output['zoom']
                st.session_state['center'] = (folium_output['center']['lat'], folium_output['center']['lng']) 
                map_neighborhood(cursor, 
                    center=st.session_state['center'], 
                    zoom_level = st.session_state['zoom'])
            else:
                map_neighborhood(cursor, center=heatmap_center, 
                                 zoom_level=st.session_state['zoom'])
        if heatmap_clicked:
            st.experimental_rerun()
    
    map_state = st.checkbox('Show timestamped outage map')
    if map_state:
        if len(hits) < 1:
            st.write('No outage to display for that location')
            map_it(cursor, center=(y, x))
        else:
            map_it(cursor, center=(y, x), last_outage_time=new_frames[-1]['Time'].iloc[0])

if __name__ == '__main__':
    main()


