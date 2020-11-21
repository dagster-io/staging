import Sidebar from "../components/DemoPage";

const Demo = () => {
  return (
    <>
      <div className="h-screen bg-white overflow-hidden flex">
        <Sidebar />
        {/* Content area */}
        <div className="flex-1 flex flex-col">
          <div className="w-full max-w-4xl mx-auto md:px-8 xl:px-0">
            <div className="relative z-10 flex-shrink-0 h-16 bg-white border-b border-gray-200 flex">
              <button className="border-r border-gray-200 px-4 text-gray-500 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-purple-500 sm:hidden">
                <span className="sr-only">Open sidebar</span>
                {/* Heroicon name: menu-alt-2 */}
                <svg
                  className="h-6 w-6"
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  aria-hidden="true"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M4 6h16M4 12h16M4 18h7"
                  />
                </svg>
              </button>
              <div className="flex-1 flex justify-between px-4 md:px-0">
                <div className="flex-1 flex">
                  <form className="w-full flex md:ml-0" action="#" method="GET">
                    <label htmlFor="search_field" className="sr-only">
                      Search
                    </label>
                    <div className="relative w-full text-gray-400 focus-within:text-gray-600">
                      <div className="pointer-events-none absolute inset-y-0 left-0 flex items-center">
                        {/* Heroicon name: search */}
                        <svg
                          className="flex-shrink-0 h-5 w-5"
                          xmlns="http://www.w3.org/2000/svg"
                          viewBox="0 0 20 20"
                          fill="currentColor"
                          aria-hidden="true"
                        >
                          <path
                            fillRule="evenodd"
                            d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z"
                            clipRule="evenodd"
                          />
                        </svg>
                      </div>
                      <input
                        id="search_field"
                        className="h-full w-full border-transparent py-2 pl-8 pr-3 text-base text-gray-900 placeholder-gray-500 focus:outline-none focus:ring-0 focus:border-transparent focus:placeholder-gray-400 sm:hidden"
                        placeholder="Search"
                        type="search"
                      />
                      <input
                        id="search_field"
                        className="hidden h-full w-full border-transparent py-2 pl-8 pr-3 text-sm text-gray-900 placeholder-gray-500 focus:outline-none focus:ring-0 focus:border-transparent focus:placeholder-gray-400 sm:block"
                        placeholder="Search jobs, applicants, and more"
                        type="search"
                      />
                    </div>
                  </form>
                </div>
                <div className="ml-4 flex items-center md:ml-6">
                  <button className="bg-white rounded-full p-1 text-gray-400 hover:bg-gray-100 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500">
                    {/* Heroicon name: bell */}
                    <svg
                      className="h-6 w-6"
                      xmlns="http://www.w3.org/2000/svg"
                      fill="none"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                      aria-hidden="true"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"
                      />
                    </svg>
                    <span className="sr-only">View notifications</span>
                  </button>
                </div>
              </div>
            </div>
          </div>
          <main
            className="flex-1 overflow-y-auto focus:outline-none"
            tabIndex={0}
          >
            <div className="relative max-w-4xl mx-auto md:px-8 xl:px-0">
              <div className="pt-10 pb-16">
                <div className="px-4 sm:px-6 md:px-0">
                  <h1 className="text-3xl font-extrabold text-gray-900">
                    Settings
                  </h1>
                </div>
                <div className="px-4 sm:px-6 md:px-0">
                  <div className="py-6">
                    {/* Tabs */}
                    <div className="lg:hidden">
                      <label htmlFor="selectedTab" className="sr-only">
                        Select a tab
                      </label>
                      <select
                        id="selectedTab"
                        className="mt-1 block w-full pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-purple-500 focus:border-purple-500 sm:text-sm rounded-md"
                      >
                        <option selected>General</option>
                        <option>Password</option>
                        <option>Notifications</option>
                        <option>Plan</option>
                        <option>Billing</option>
                        <option>Team Members</option>
                      </select>
                    </div>
                    <div className="hidden lg:block">
                      <div className="border-b border-gray-200">
                        <nav className="-mb-px flex">
                          <a
                            href="#"
                            className="whitespace-nowrap py-4 px-1 border-b-2 border-purple-500 font-medium text-sm text-purple-600"
                          >
                            General
                          </a>
                          <a
                            href="#"
                            className="whitespace-nowrap ml-8 py-4 px-1 border-b-2 border-transparent font-medium text-sm text-gray-500 hover:text-gray-700 hover:border-gray-300"
                          >
                            Password
                          </a>
                          <a
                            href="#"
                            className="whitespace-nowrap ml-8 py-4 px-1 border-b-2 border-transparent font-medium text-sm text-gray-500 hover:text-gray-700 hover:border-gray-300"
                          >
                            Notifications
                          </a>
                          <a
                            href="#"
                            className="whitespace-nowrap ml-8 py-4 px-1 border-b-2 border-transparent font-medium text-sm text-gray-500 hover:text-gray-700 hover:border-gray-300"
                          >
                            Plan
                          </a>
                          <a
                            href="#"
                            className="whitespace-nowrap ml-8 py-4 px-1 border-b-2 border-transparent font-medium text-sm text-gray-500 hover:text-gray-700 hover:border-gray-300"
                          >
                            Billing
                          </a>
                          <a
                            href="#"
                            className="whitespace-nowrap ml-8 py-4 px-1 border-b-2 border-transparent font-medium text-sm text-gray-500 hover:text-gray-700 hover:border-gray-300"
                          >
                            Team Members
                          </a>
                        </nav>
                      </div>
                    </div>
                    {/* Description list with inline editing */}
                    <div className="mt-10 divide-y divide-gray-200">
                      <div className="space-y-1">
                        <h3 className="text-lg leading-6 font-medium text-gray-900">
                          Profile
                        </h3>
                        <p className="max-w-2xl text-sm text-gray-500">
                          This information will be displayed publicly so be
                          careful what you share.
                        </p>
                      </div>
                      <div className="mt-6">
                        <dl className="divide-y divide-gray-200">
                          <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4">
                            <dt className="text-sm font-medium text-gray-500">
                              Name
                            </dt>
                            <dd className="mt-1 flex text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                              <span className="flex-grow">Chelsea Hagon</span>
                              <span className="ml-4 flex-shrink-0">
                                <button
                                  type="button"
                                  className="bg-white rounded-md font-medium text-purple-600 hover:text-purple-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500"
                                >
                                  Update
                                </button>
                              </span>
                            </dd>
                          </div>
                          <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:pt-5">
                            <dt className="text-sm font-medium text-gray-500">
                              Photo
                            </dt>
                            <dd className="mt-1 flex text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                              <span className="flex-grow">
                                <img
                                  className="h-8 w-8 rounded-full"
                                  src="https://images.unsplash.com/photo-1550525811-e5869dd03032?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=facearea&facepad=2&w=256&h=256&q=80"
                                />
                              </span>
                              <span className="ml-4 flex-shrink-0 flex items-start space-x-4">
                                <button
                                  type="button"
                                  className="bg-white rounded-md font-medium text-purple-600 hover:text-purple-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500"
                                >
                                  Update
                                </button>
                                <span
                                  className="text-gray-300"
                                  aria-hidden="true"
                                >
                                  |
                                </span>
                                <button
                                  type="button"
                                  className="bg-white rounded-md font-medium text-purple-600 hover:text-purple-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500"
                                >
                                  Remove
                                </button>
                              </span>
                            </dd>
                          </div>
                          <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:pt-5">
                            <dt className="text-sm font-medium text-gray-500">
                              Email
                            </dt>
                            <dd className="mt-1 flex text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                              <span className="flex-grow">
                                chelsea.hagon@example.com
                              </span>
                              <span className="ml-4 flex-shrink-0">
                                <button
                                  type="button"
                                  className="bg-white rounded-md font-medium text-purple-600 hover:text-purple-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500"
                                >
                                  Update
                                </button>
                              </span>
                            </dd>
                          </div>
                          <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:border-b sm:border-gray-200">
                            <dt className="text-sm font-medium text-gray-500">
                              Job title
                            </dt>
                            <dd className="mt-1 flex text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                              <span className="flex-grow">
                                Human Resources Manager
                              </span>
                              <span className="ml-4 flex-shrink-0">
                                <button
                                  type="button"
                                  className="bg-white rounded-md font-medium text-purple-600 hover:text-purple-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500"
                                >
                                  Update
                                </button>
                              </span>
                            </dd>
                          </div>
                        </dl>
                      </div>
                    </div>
                    <div className="mt-10 divide-y divide-gray-200">
                      <div className="space-y-1">
                        <h3 className="text-lg leading-6 font-medium text-gray-900">
                          Account
                        </h3>
                        <p className="max-w-2xl text-sm text-gray-500">
                          Manage how information is displayed on your account.
                        </p>
                      </div>
                      <div className="mt-6">
                        <dl className="divide-y divide-gray-200">
                          <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4">
                            <dt className="text-sm font-medium text-gray-500">
                              Language
                            </dt>
                            <dd className="mt-1 flex text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                              <span className="flex-grow">English</span>
                              <span className="ml-4 flex-shrink-0">
                                <button
                                  type="button"
                                  className="bg-white rounded-md font-medium text-purple-600 hover:text-purple-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500"
                                >
                                  Update
                                </button>
                              </span>
                            </dd>
                          </div>
                          <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:pt-5">
                            <dt className="text-sm font-medium text-gray-500">
                              Date format
                            </dt>
                            <dd className="mt-1 flex text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                              <span className="flex-grow">DD-MM-YYYY</span>
                              <span className="ml-4 flex-shrink-0 flex items-start space-x-4">
                                <button
                                  type="button"
                                  className="bg-white rounded-md font-medium text-purple-600 hover:text-purple-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500"
                                >
                                  Update
                                </button>
                                <span
                                  className="text-gray-300"
                                  aria-hidden="true"
                                >
                                  |
                                </span>
                                <button
                                  type="button"
                                  className="bg-white rounded-md font-medium text-purple-600 hover:text-purple-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500"
                                >
                                  Remove
                                </button>
                              </span>
                            </dd>
                          </div>
                          <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:pt-5">
                            <dt
                              id="timezone-option-label"
                              className="text-sm font-medium text-gray-500"
                            >
                              Automatic timezone
                            </dt>
                            <dd className="mt-1 flex text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                              {/* On: "bg-purple-600", Off: "bg-gray-200" */}
                              <button
                                type="button"
                                aria-pressed="true"
                                aria-labelledby="timezone-option-label"
                                className="bg-gray-200 relative inline-flex flex-shrink-0 h-6 w-11 border-2 border-transparent rounded-full cursor-pointer transition-colors ease-in-out duration-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500 sm:ml-auto"
                              >
                                <span className="sr-only">Use setting</span>
                                {/* On: "translate-x-5", Off: "translate-x-0" */}
                                <span
                                  aria-hidden="true"
                                  className="translate-x-0 inline-block h-5 w-5 rounded-full bg-white shadow transform ring-0 transition ease-in-out duration-200"
                                />
                              </button>
                            </dd>
                          </div>
                          <div className="py-4 sm:py-5 sm:grid sm:grid-cols-3 sm:gap-4 sm:border-b sm:border-gray-200">
                            <dt
                              id="auto-update-option-label"
                              className="text-sm font-medium text-gray-500"
                            >
                              Auto-update applicant data
                            </dt>
                            <dd className="mt-1 flex text-sm text-gray-900 sm:mt-0 sm:col-span-2">
                              {/* On: "bg-purple-600", Off: "bg-gray-200" */}
                              <button
                                type="button"
                                aria-pressed="false"
                                aria-labelledby="auto-update-option-label"
                                className="bg-gray-200 relative inline-flex flex-shrink-0 h-6 w-11 border-2 border-transparent rounded-full cursor-pointer transition-colors ease-in-out duration-200 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500 sm:ml-auto"
                              >
                                <span className="sr-only">Use setting</span>
                                {/* On: "translate-x-5", Off: "translate-x-0" */}
                                <span
                                  aria-hidden="true"
                                  className="translate-x-0 inline-block h-5 w-5 rounded-full bg-white shadow transform ring-0 transition ease-in-out duration-200"
                                />
                              </button>
                            </dd>
                          </div>
                        </dl>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </main>
        </div>
      </div>
    </>
  );
};

export default Demo;
