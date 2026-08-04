// The test's known image: a red/green gradient over the image, with a filled
// circle in the middle whose blue channel is inverted. Both features are what
// vulkan_test.cc asserts on -- the gradient proves the screenshot is oriented
// and scaled correctly, the circle proves it is the drawn content rather than a
// cleared buffer.
RWTexture2D<float4> image;

[shader("compute")]
[numthreads(16, 16, 1)]
void computeMain(
    uint3 globalID : SV_DispatchThreadID,
    uint3 localID : SV_GroupThreadID
)
{
    int2 texelCoord = int2(globalID.xy);
    int2 size;
    image.GetDimensions(size.x, size.y);

    if(texelCoord.x < size.x && texelCoord.y < size.y)
    {
        // Blue is 0 outside the circle and 1 inside it, so the two regions are
        // unambiguous in the screenshot even after a linear downscale.
        float4 color = float4(0.0, 0.0, 0.0, 1.0);

        color.x = float(texelCoord.x) / size.x;
        color.y = float(texelCoord.y) / size.y;

        float dx = texelCoord.x - size.x/2;
        float dy = texelCoord.y - size.y/2;
        // Proportional to the image so the circle survives the downscale to the
        // screenshot buffer at any window size.
        float r = min(size.x, size.y) / 4.0;
        if (dx*dx + dy*dy <= r*r) {
            color.z = 1.0;
        }
        image[texelCoord] = color;
    }
}
