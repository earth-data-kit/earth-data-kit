import { BitmapLayer } from "@deck.gl/layers";

async function GetRasterBitmapLayer(img_url, bounds) {
  const imageData = await fetchImageData(img_url);

  const bitmapLayer = new BitmapLayer({
    id: "RasterBitmapLayer",
    bounds: bounds,
    image: imageData,
    opacity: 0.7,
    pickable: true,
  });

  return bitmapLayer;
}

async function fetchImageData(img_url) {
  try {
    const response = await fetch(img_url);
    if (!response.ok) {
      throw new Error(
        `Failed to fetch image: ${response.status} ${response.statusText}`,
      );
    }

    const blob = await response.blob();
    const bitmap = await createImageBitmap(blob);

    // Create a canvas to draw the bitmap
    const canvas = document.createElement("canvas");
    canvas.width = bitmap.width;
    canvas.height = bitmap.height;
    const ctx = canvas.getContext("2d");

    // Draw the bitmap on the canvas
    ctx.drawImage(bitmap, 0, 0, bitmap.width, bitmap.height);

    // Get the image data from the canvas
    const imageData = ctx.getImageData(0, 0, bitmap.width, bitmap.height);

    return imageData;
  } catch (error) {
    console.error("Error fetching image data:", error);
    throw error;
  }
}

export default GetRasterBitmapLayer;
