class ResizeObserverMock {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

globalThis.ResizeObserver = ResizeObserverMock;

Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false
  })
});

const getComputedStyleMock = window.getComputedStyle.bind(window);
const textareaSizingProperties = new Set([
  "padding-top",
  "padding-bottom",
  "border-top-width",
  "border-bottom-width"
]);

Object.defineProperty(window, "getComputedStyle", {
  writable: true,
  value: (element: Element): CSSStyleDeclaration => {
    const styles = getComputedStyleMock(element);

    if (!(element instanceof HTMLTextAreaElement)) {
      return styles;
    }

    const getPropertyValue = styles.getPropertyValue.bind(styles);
    return new Proxy(styles, {
      get(target, property) {
        if (property === "getPropertyValue") {
          return (name: string) => {
            const value = getPropertyValue(name);
            return value || (textareaSizingProperties.has(name) ? "0px" : value);
          };
        }

        const value = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      }
    });
  }
});
