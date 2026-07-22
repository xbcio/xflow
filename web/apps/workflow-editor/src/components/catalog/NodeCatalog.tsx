import { DEFAULT_CATALOG, filterCatalog } from "../../utils/catalog";
import { useEditor } from "../../context/EditorContext";

export function NodeCatalog() {
  const { catalogKeyword, setCatalogKeyword } = useEditor();
  const filtered = filterCatalog(DEFAULT_CATALOG, catalogKeyword);

  const grouped = filtered.reduce<Record<string, typeof filtered>>((acc, item) => {
    const category = item.category;
    if (!acc[category]) {
      acc[category] = [];
    }
    acc[category].push(item);
    return acc;
  }, {});

  const categories = Object.keys(grouped).sort((a, b) => a.localeCompare(b));

  function handleDragStart(event: React.DragEvent<HTMLDivElement>, type: string): void {
    event.dataTransfer.setData("application/json", JSON.stringify({ type }));
    event.dataTransfer.effectAllowed = "copy";
  }

  return (
    <div className="node-catalog" data-testid="node-catalog">
      <div className="node-catalog__header">
        <h3>Node Catalog</h3>
      </div>
      <div className="node-catalog__search">
        <input
          type="text"
          placeholder="Search nodes..."
          value={catalogKeyword}
          onChange={(event) => setCatalogKeyword(event.target.value)}
          data-testid="catalog-search"
        />
      </div>
      <div className="node-catalog__list">
        {categories.length === 0 ? (
          <div className="node-catalog__empty" data-testid="catalog-empty">
            No nodes match your search.
          </div>
        ) : (
          categories.map((category) => (
            <div key={category} className="node-catalog__category">
              <div className="node-catalog__category-title">{category}</div>
              <div className="node-catalog__items">
                {grouped[category].map((item) => (
                  <div
                    key={item.type}
                    className="node-catalog__item"
                    draggable
                    onDragStart={(event) => handleDragStart(event, item.type)}
                    data-testid={`catalog-item-${item.type}`}
                  >
                    {item.icon ? (
                      <span className="node-catalog__item-icon">{item.icon}</span>
                    ) : null}
                    <span className="node-catalog__item-label">{item.label}</span>
                  </div>
                ))}
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
