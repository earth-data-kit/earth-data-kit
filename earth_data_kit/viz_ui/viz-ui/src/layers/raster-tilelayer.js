import { TileLayer } from "@deck.gl/geo-layers";
import { BitmapLayer } from "@deck.gl/layers";

function GetRasterTileLayer(img_url) {
  const tileLayer = new TileLayer({
    id: "RasterTileLayer",
    data: `${img_url}/tile/{z}/{x}/{y}.png`,
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
    pickable: true
  });

  return tileLayer;
}

export default GetRasterTileLayer;
