import json

from shapely import wkt, GEOSException


def print_coordinate(place_id: str, coordinate: list) -> None:
    s = (
            f"const {place_id} = ["
            + ",".join(
        list(map(lambda x: f"{{lat:{x[1]},lng:{x[0]}}}", coordinate))
    )
            + "];\n"
    )
    s += f'const {place_id}_p = new google.maps.Polygon({{paths: {place_id}, strokeColor: ' \
         f'"#FF0000", strokeOpacity: 0.8, strokeWeight: 3, fillColor: "#FF0000", fillOpacity: 0.35,}});\n' \
         f'{place_id}_p.setMap(map);'
    print(s)


def validate_polygon(coordinates: list) -> None:
    # validate polygon
    try:
        polygon_coordinates = [f"{o[1]} {o[0]}" for o in coordinates]
        polygon_data = f'Polygon(({",".join(polygon_coordinates)}))'
        wkt.loads(polygon_data)
    except GEOSException:
        print(coordinates)


def main():
    with open("GB.json", "r") as f:
        geometries_raw = json.load(f)
        for entry in geometries_raw:
            place_id = entry["place_id"]
            e_coordinates = entry["coordinates"]
            if type(e_coordinates) == str:
                coordinates = json.loads(e_coordinates)
                validate_polygon(coordinates)
            elif type(e_coordinates) == list:
                coordinates = []
                for c in e_coordinates:
                    coordinate = json.loads(c)
                    if type(coordinate[0][0]) == list:
                        coordinate = coordinate[0]
                    validate_polygon(coordinate)
                    coordinates.append(coordinate)
            loop = False
            if type(coordinates[0][0]) == list:
                loop = True
            if loop:
                for i, coordinate in enumerate(coordinates):
                    print_coordinate(f'{place_id}_{i}', coordinate)
            else:
                print_coordinate(place_id, coordinates)


if __name__ == "__main__":
    main()
