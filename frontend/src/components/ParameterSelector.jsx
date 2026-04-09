export default function ParameterSelector({ items, selectedItem, onChange }) {
  return (
    <div className="selector-wrapper">
      <label className="selector-label" htmlFor="param-select">
        Parameter
      </label>
      <select
        id="param-select"
        className="selector"
        value={selectedItem ?? ''}
        onChange={(e) => onChange(e.target.value)}
      >
        {items.map((item) => (
          <option key={item.item} value={item.item}>
            {item.label} ({item.unit})
          </option>
        ))}
      </select>
    </div>
  )
}
