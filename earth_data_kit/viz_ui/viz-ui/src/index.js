import { Deck } from "@deck.gl/core";
import GetOSMLayer from "./layers/osm";
import GetRasterBitmapLayer from "./layers/raster-bitmap";

const BASE_URL = "http://localhost:5432";
const layers = [GetOSMLayer()];

var deckInstance;

fetch(`${BASE_URL}/bounds`)
  .then((response) => response.json())
  .then((bbox) => {
    const centerLon = (bbox.bbox[0] + bbox.bbox[2]) / 2;
    const centerLat = (bbox.bbox[1] + bbox.bbox[3]) / 2;

    deckInstance = new Deck({
      initialViewState: {
        longitude: centerLon,
        latitude: centerLat,
        zoom: 3,
      },
      controller: true,
      getTooltip: ({ bitmap }) => {
        return bitmap && `${bitmap.pixel}`;
      },
      layers: [GetOSMLayer()],
    });

    GetRasterBitmapLayer(`${BASE_URL}/image`, bbox.bbox).then((layer) => {
      layers.push(layer);
      deckInstance.setProps({ layers: layers });
      setLoaded();
    });
  });
