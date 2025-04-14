import { BitmapLayer } from "@deck.gl/layers";
import { TileLayer } from "@deck.gl/geo-layers";

function GetOSMLayer() {
    const osmLayer = new TileLayer({
        id: "BaseMap",
        data: "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
        maxZoom: 19,
        minZoom: 0,

        renderSubLayers: (props) => {
            const { boundingBox } = props.tile;

            return new BitmapLayer(props, {
                data: null,
                image: props.data,
                bounds: [
                    boundingBox[0][0],
                    boundingBox[0][1],
                    boundingBox[1][0],
                    boundingBox[1][1],
                ],
            });
        },
        pickable: true,
    });

    return osmLayer;
}

export default GetOSMLayer;
