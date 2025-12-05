import { createCanvas } from "canvas";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Polyfill HTMLCanvasElement untuk pdf.js SEBELUM library lain di-import
class CanvasPolyfill {
    constructor(width = 300, height = 150) {
        this._canvas = createCanvas(width, height);
        this.width = width;
        this.height = height;
    }
    
    get width() {
        return this._canvas.width;
    }
    
    set width(value) {
        this._canvas.width = value;
    }
    
    get height() {
        return this._canvas.height;
    }
    
    set height(value) {
        this._canvas.height = value;
    }
    
    getContext(type, options) {
        if (type === "2d") {
            return this._canvas.getContext("2d", options);
        }
        return null;
    }
    
    toDataURL(type, quality) {
        return this._canvas.toDataURL(type, quality);
    }
    
    toBuffer(type) {
        return this._canvas.toBuffer(type);
    }
}

global.HTMLCanvasElement = CanvasPolyfill;

// Polyfill document.createElement untuk mendukung canvas, div, span
global.document = {
    createElement: (tag) => {
        if (tag === 'canvas') {
            return createCanvas(300, 150);
        }
        // Return mock DOM element for div/span
        return {
            setAttribute: (name, value) => {
                this[name] = value;
            },
            appendChild: (child) => {
                // no-op
            },
            style: {},
            textContent: '',
            getContext: () => null
        };
    },
    body: {
        appendChild: (child) => {
            // no-op
        },
        removeChild: (child) => {
            // no-op
        }
    }
};

export { CanvasPolyfill };
