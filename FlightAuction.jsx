import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import Navbar from "../../Homepage/Navbar/Navbar";

// Icons for AuctionNav
import { BsAirplane } from "react-icons/bs";
import { RiHotelLine } from "react-icons/ri";
import { GrTicket } from "react-icons/gr";
import { LuTrainFrontTunnel } from "react-icons/lu";
import { GoArrowSwitch } from "react-icons/go";
import { IoTimeOutline } from "react-icons/io5";
import "./flightauction.css";

// 💡 1. Import the centralized auction data file.
import { allAuctionDeals } from '../../../Data/AuctionData';

// 💡 2. Filter the imported data to get only "Flights" deals.
const flightDeals = allAuctionDeals.filter(deal => deal.category === "Flights");

const AuctionDeals = () => {
    const navigate = useNavigate();
    const [from, setFrom] = useState("");
    const [to, setTo] = useState("");
    const [departure, setDeparture] = useState("");
    const [sortBy, setSortBy] = useState("All");

    // 💡 3. Initialize state with the filtered flight deals.
    const [filteredDeals, setFilteredDeals] = useState(flightDeals);
    const [visibleDeals, setVisibleDeals] = useState(flightDeals.slice(0, 3));
    const [visibleCount, setVisibleCount] = useState(3);

    const auctionNavIcons = [
        { label: "Flights", icon: <BsAirplane size={80} />, path: "/auction/flight" },
        { label: "Hotels", icon: <RiHotelLine size={80} />, path: "/auction/hotel" },
        { label: "Tours & Tickets", icon: <GrTicket size={80} />, path: "/auction/tickets" },
        { label: "Transportation", icon: <LuTrainFrontTunnel size={70} />, path: "/auction/transportation" }
    ];

    const sortDeals = (deals, criteria) => {
        let sorted = [...deals];
        if (criteria === "Newest") sorted.sort((a, b) => new Date(b.departureDate) - new Date(a.departureDate));
        else if (criteria === "Oldest") sorted.sort((a, b) => new Date(a.departureDate) - new Date(b.departureDate));
        else if (criteria === "Popular") sorted.sort((a, b) => parseInt(b.bids) - parseInt(a.bids));
        else if (criteria === "Price") sorted.sort((a, b) => a.price - b.price);
        return sorted;
    };

    useEffect(() => {
        const sortedAndFiltered = sortDeals(filteredDeals, sortBy);
        setVisibleDeals(sortedAndFiltered.slice(0, visibleCount));
    }, [sortBy, visibleCount, filteredDeals]);

    const handleSearch = () => {
        const lowerFrom = from.toLowerCase();
        const lowerTo = to.toLowerCase();

        // 💡 4. The search logic now filters the flightDeals array.
        const filtered = flightDeals.filter((deal) => {
            const [depSegment, arrSegment] = deal.time.split("→");
            const depCity = depSegment?.trim().split(" ").slice(-1)[0].toLowerCase();
            const arrCity = arrSegment?.trim().split(" ").slice(-1)[0].toLowerCase();

            const matchesFrom = !from || depCity === lowerFrom;
            const matchesTo = !to || arrCity === lowerTo;
            const matchesDate = !departure || deal.departureDate === departure;

            return matchesFrom && matchesTo && matchesDate;
        });

        setFilteredDeals(filtered);
        setVisibleCount(3);
    };

    const handleSwap = () => {
        const temp = from;
        setFrom(to);
        setTo(temp);

        // A small delay ensures the state updates before the search runs.
        setTimeout(handleSearch, 0);
    };

    const handleSeeMore = () => {
        const nextCount = visibleCount + 3;
        setVisibleCount(nextCount);
    };

    const handleFlightClick = (flightId) => {
        navigate(`/flight-details/${flightId}`);
    };

    // Helper function to format departure date
    const formatDepartureDate = (dateString) => {
        const date = new Date(dateString);
        return date.toLocaleDateString('en-US', { 
            month: 'short', 
            day: 'numeric'
        });
    };

    return (
        <div className="flight-auction-app">
            <Navbar />
            <div className="flight-app-header">
                <h1>Your Journey, Your Price.</h1>
            </div>

            <div className="create-auction-btn-container">
    <button
        className="create-auction-btn"
        onClick={() => navigate("/publish-auction")}
    >
        Create Auction
    </button>
</div>


            <div className="auction-nav-integrated">
                <div className="nav-icons">
                    {auctionNavIcons.map((item) => (
                        <button key={item.label} className="icon-btn" onClick={() => navigate(item.path)}>
                            {item.icon}
                            <span>{item.label}</span>
                        </button>
                    ))}
                </div>
            </div>

            <div className="search-container">
                <div className="search-row">
                    <div className="search-input">
                        <label>From</label>
                        <input
                            type="text"
                            placeholder="Departure City"
                            value={from}
                            onChange={(e) => setFrom(e.target.value)}
                        />
                    </div>
                    <span className="swap-arrow" onClick={handleSwap}>
                        <GoArrowSwitch />
                    </span>
                    <div className="search-input">
                        <label>To</label>
                        <input
                            type="text"
                            placeholder="Arrival City"
                            value={to}
                            onChange={(e) => setTo(e.target.value)}
                        />
                    </div>
                </div>

                <div className="search-input full-width">
                    <label>Departure</label>
                    <input
                        type="date"
                        value={departure}
                        onChange={(e) => setDeparture(e.target.value)}
                    />
                </div>

                <div className="search-btn">
                    <button className="searchbtn" onClick={handleSearch}>
                        Search
                    </button>
                </div>
            </div>

            <div className="deals-section">
                <div className="sort-section">
                    <div className="sort-header">
                        <h2 className="sort-title">Best Flight Deals</h2>
                    </div>
                    <div className="sort-by">
                        <label htmlFor="sort">Sort by:</label>
                        <select
                            id="sort"
                            value={sortBy}
                            onChange={(e) => setSortBy(e.target.value)}
                        >
                            <option>All</option>
                            <option>Newest</option>
                            <option>Oldest</option>
                            <option>Popular</option>
                            <option>Price</option>
                        </select>
                    </div>
                </div>

                <div className="deals-list">
                    {visibleDeals.length === 0 ? (
                        <div className="no-deals-message">
                            No deals found for your search.
                        </div>
                    ) : (
                        visibleDeals.map((deal) => (
                            <div
                                className="deal-card clickable"
                                key={deal.id}
                                onClick={() => handleFlightClick(deal.id)}
                            >
                                <img src={deal.img} alt={deal.title} className="deal-images" />
                                <div className="deal-info">
                                    <div className="deal-title">{deal.title}</div>
                                    <div className="deal-details">
                                        <div className="deal-time">
                                            <strong>{deal.time.split("→")[0].trim()}</strong> →
                                            <strong>{deal.time.split("→")[1].trim()}</strong>
                                        </div>
                                        <div className="deal-meta">
                                            <span>{deal.bids} bids</span>
                                            {deal.duration && (
                                                <span className="deal-duration">
                                                    <IoTimeOutline style={{ marginRight: '4px' }} />
                                                    {deal.duration}
                                                </span>
                                            )}
                                        </div>
                                    </div>
                                </div>
                                <div className="deal-price">
                                    <div className="eth">{deal.price.toFixed(3)} ETH</div>
                                    <div className="date">{formatDepartureDate(deal.departureDate)}</div>
                                    {deal.duration && (
                                        <div className="duration-badge">{deal.duration}</div>
                                    )}
                                </div>
                            </div>
                        ))
                    )}
                </div>

                {visibleCount < filteredDeals.length && (
                    <button className="see-more-btn" onClick={handleSeeMore}>
                        See More
                    </button>
                )}
            </div>
        </div>
    );
};

export default AuctionDeals;