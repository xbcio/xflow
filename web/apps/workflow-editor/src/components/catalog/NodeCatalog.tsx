import { useMemo, useRef, useState } from "react";
import { DEFAULT_CATALOG, filterCatalog, getCatalogIcon } from "../../utils/catalog";
import { useEditor } from "../../context/EditorContext";

const categoryStyleMap: Record<string, string> = {
  控制: "bg-blue-500/10 text-blue-500 dark:bg-blue-400/[0.12] dark:text-blue-400",
  流程: "bg-violet-500/10 text-violet-500 dark:bg-violet-400/[0.12] dark:text-violet-400",
  动作: "bg-cyan-500/10 text-cyan-500 dark:bg-cyan-400/[0.12] dark:text-cyan-400",
  "等待/延时": "bg-yellow-500/10 text-yellow-500 dark:bg-yellow-400/[0.12] dark:text-yellow-400",
};

const iconBaseClass = "w-8 h-8 rounded-md flex items-center justify-center";
const fallbackClass = "text-[10px] font-semibold uppercase tracking-[0.02em]";

export function NodeCatalog() {
  const { catalogKeyword, setCatalogKeyword } = useEditor();
  const [searchVisible, setSearchVisible] = useState(false);
  const [collapsedCategories, setCollapsedCategories] = useState<Set<string>>(new Set());
  const searchInputRef = useRef<HTMLInputElement>(null);

  const filtered = filterCatalog(DEFAULT_CATALOG, catalogKeyword);

  const grouped = useMemo(() => {
    return filtered.reduce<Record<string, typeof filtered>>((acc, item) => {
      const category = item.category;
      if (!acc[category]) {
        acc[category] = [];
      }
      acc[category].push(item);
      return acc;
    }, {});
  }, [filtered]);

  const categories = Object.keys(grouped).sort((a, b) => a.localeCompare(b));

  function handleToggleSearch() {
    setSearchVisible((prev) => {
      const next = !prev;
      if (next) {
        setTimeout(() => searchInputRef.current?.focus(), 0);
      }
      return next;
    });
  }

  function handleToggleCategory(category: string) {
    setCollapsedCategories((prev) => {
      const next = new Set(prev);
      if (next.has(category)) {
        next.delete(category);
      } else {
        next.add(category);
      }
      return next;
    });
  }

  function handleDragStart(event: React.DragEvent<HTMLDivElement>, type: string): void {
    event.dataTransfer.setData("application/json", JSON.stringify({ type }));
    event.dataTransfer.effectAllowed = "copy";
  }

  return (
    <div className="flex flex-col h-full" data-testid="node-catalog">
      <div className="flex items-center justify-between px-3 py-2 border-b border-editor-border">
        <div className="flex items-center gap-2">
          <svg className="w-4 h-4 text-editor-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z" />
          </svg>
          <span className="text-sm font-medium text-editor-text">节点</span>
        </div>
        <button
          type="button"
          className="inline-flex items-center justify-center w-8 h-8 rounded text-editor-text-secondary hover:text-editor-text hover:bg-editor-hover transition-colors"
          onClick={handleToggleSearch}
          aria-label="搜索节点"
          title="搜索节点"
          data-testid="catalog-search-toggle"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        </button>
      </div>

      <div className={`px-3 py-2 border-b border-editor-border ${searchVisible ? "" : "hidden"}`} data-testid="catalog-search-box">
        <div className="relative">
          <svg className="w-3.5 h-3.5 absolute left-2 top-1/2 -translate-y-1/2 text-editor-text-secondary" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            ref={searchInputRef}
            type="text"
            placeholder="搜索节点..."
            value={catalogKeyword}
            onChange={(event) => setCatalogKeyword(event.target.value)}
            className="w-full py-1 pl-7 pr-2 text-xs leading-5 bg-editor-input border border-editor-border rounded-md text-editor-text outline-none focus:border-editor-accent"
            data-testid="catalog-search"
          />
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-auto p-2">
        {categories.length === 0 ? (
          <div className="px-2 py-1.5 text-sm text-editor-text-secondary" data-testid="catalog-empty">
            没有匹配的节点
          </div>
        ) : (
          categories.map((category) => {
            const collapsed = collapsedCategories.has(category);
            return (
              <div key={category} className={`mb-1 ${collapsed ? "collapsed" : ""}`}>
                <button
                  type="button"
                  className="flex items-center gap-2 w-full px-3 py-1.5 text-editor-text-secondary rounded cursor-pointer transition-colors hover:bg-editor-hover hover:text-editor-text"
                  onClick={() => handleToggleCategory(category)}
                  aria-expanded={!collapsed}
                >
                  <svg className={`w-3 h-3 transition-transform ${collapsed ? "-rotate-90" : ""}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                  <span className="text-xs font-semibold uppercase">{category}</span>
                  <span className="ml-auto text-[10px] text-editor-muted">{grouped[category].length}</span>
                </button>
                <div className={`px-2 pb-1 ${collapsed ? "hidden" : ""}`}>
                  <div className="grid grid-cols-3 gap-2">
                    {grouped[category].map((item) => {
                      const icon = getCatalogIcon(item.type);
                      const categoryStyle = categoryStyleMap[category] ?? categoryStyleMap["动作"];
                      return (
                        <div
                          key={item.type}
                          className="flex flex-col items-center gap-1.5 py-2.5 px-1 rounded-md border border-editor-border bg-editor-input cursor-grab transition-colors hover:bg-editor-surface hover:border-editor-border shadow-[0_1px_2px_rgba(0,0,0,0.04)] dark:hover:shadow-[0_1px_2px_rgba(0,0,0,0.2)]"
                          draggable
                          onDragStart={(event) => handleDragStart(event, item.type)}
                          data-testid={`catalog-item-${item.type}`}
                        >
                          <div className={`${iconBaseClass} ${categoryStyle} ${icon ? "" : fallbackClass}`}>
                            {icon ?? item.label.slice(0, 2)}
                          </div>
                          <span className="text-[10px] leading-none text-center text-editor-text max-w-full px-1 whitespace-nowrap overflow-hidden text-ellipsis">
                            {item.label}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
